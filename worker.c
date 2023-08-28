/*---------------------------------------------------------
 *
 * worker.c
 *     Background worker to call functions of pg_squeeze.c
 *
 * Copyright (c) 2016-2023, CYBERTEC PostgreSQL International GmbH
 *
 *---------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xact.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "replication/slot.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/lock.h"
#if PG_VERSION_NUM >= 160000
#include "utils/backend_status.h"
#endif
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#if PG_VERSION_NUM < 120000
#include "utils/rel.h"
#endif
#include "utils/snapmgr.h"

#include "pg_squeeze.h"

/*
 * The shmem_request_hook_type hook was introduced in PG 15. Since the number
 * of slots depends on the max_worker_processes GUC, the maximum number of
 * squeeze workers must be a compile time constant for PG < 15.
 *
 * XXX Maybe we don't need to worry about dependency on an in-core GUC - the
 * value should be known at the load time and no loadable module should be
 * able to change it.
 */
static int
max_squeeze_workers(void)
{
#if PG_VERSION_NUM >= 150000
	return max_worker_processes;
#else
#define	MAX_SQUEEZE_WORKERS	8

	/*
	 * If max_worker_processes appears to be greater than MAX_SQUEEZE_WORKERS,
	 * postmaster can start new processes but squeeze_worker_main() will fail
	 * to find a slot for them, and therefore those extra workers will exit
	 * immediately.
	 */
	return MAX_SQUEEZE_WORKERS;
#endif
}

/*
 * Shared memory structures to keep track of the status of squeeze workers.
 */
typedef struct WorkerSlot
{
	Oid			dbid;			/* database the worker is connected to */
	Oid			relid;			/* relation the worker is working on */
	int			pid;			/* the PID */
	bool		scheduler;		/* true if scheduler, false if the "squeeze
								 * worker" */
	WorkerProgress progress;	/* progress tracking information */
	Latch	   *latch;			/* use this to wake up the worker */
} WorkerSlot;

/*
 * This structure represents a task assigned to the worker via shared memory.
 */
typedef struct WorkerTask
{
	int			id;				/* the task id */

	Oid			dbid;			/* also indicates whether the task is valid */
	bool		in_progress;	/* for workers to avoid processing the same
								 * task */
	NameData	relschema;
	NameData	relname;
	NameData	indname;		/* clustering index */
	NameData	tbspname;		/* destination tablespace */

	/*
	 * index destination tablespaces.
	 *
	 * text[][] array is stored here. The space should only be used by the
	 * interactive squeeze_table() function, which is only there for testing
	 * and troubleshooting purposes. If the array doesn't fit here, the user
	 * needs to use the regular UI (ie register the table for squeezing and
	 * insert a record into the "tasks" table).
	 */
#define IND_TABLESPACES_ARRAY_SIZE	1024
	char		ind_tbsps[IND_TABLESPACES_ARRAY_SIZE];

	/*
	 * Exclusive lock is needed to change the contents. Note that for the
	 * worker the change includes the actual work.
	 */
	LWLock	   *lock;

	/*
	 * The worker uses this to notify backends that the next task can be
	 * assigned.
	 */
	ConditionVariable cv;
} WorkerTask;

/*
 * The maximum number of tasks submitted by the squeeze_table() user function
 * that can be in progress at a time (as long as there's enough workers). Note
 * that this is cluster-wide constant.
 */
#define	NUM_WORKER_TASKS	8

typedef struct WorkerData
{
	WorkerTask	tasks[NUM_WORKER_TASKS];

	/* The next task for a backend to use is (next_task % NUM_WORKER_TASKS) */
	pg_atomic_uint32 next_task;

	/*
	 * A lock to synchronize access to slots. Lock in exclusive mode to add /
	 * remove workers, in shared mode to find information on them.
	 */
	LWLock	   *lock;

	int			nslots;			/* size of the array */
	WorkerSlot	slots[FLEXIBLE_ARRAY_MEMBER];
} WorkerData;

static WorkerData *workerData = NULL;

/* Local pointer to the slot in the shared memory. */
static WorkerSlot *MyWorkerSlot = NULL;

/* Local pointer to the task in the shared memory. */
static WorkerTask *MyWorkerTask = NULL;

/* Local pointer to the progress information. */
WorkerProgress *MyWorkerProgress = NULL;

static void reset_progress(WorkerProgress *progress);

/* Information retrieved from the "tasks" and "tables" functions. */
typedef struct TaskDetails
{
	uint32		id;
	NameData	relschema;
	NameData	relname;
	NameData	cl_index;
	NameData	rel_tbsp;
	ArrayType  *ind_tbsps;
	bool		last_try;
	bool		skip_analyze;
} TaskDetails;

static void init_task_details(TaskDetails *task, int32 task_id,
							  Name relschema, Name relname, Name cl_index,
							  Name rel_tbsp, ArrayType *ind_tbsps,
							  bool last_try, bool skip_analyze);
static void squeeze_handle_error_app(ErrorData *edata, TaskDetails *td);
static void release_task(void);

static Size
worker_shmem_size(void)
{
	Size		size;

	size = offsetof(WorkerData, slots);
	size = add_size(size, mul_size(max_squeeze_workers(),
								   sizeof(WorkerSlot)));
	return size;
}

void
squeeze_worker_shmem_request(void)
{
	/* With lower PG versions this function is called from _PG_init(). */
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif							/* PG_VERSION_NUM >= 150000 */

	RequestAddinShmemSpace(worker_shmem_size());
	RequestNamedLWLockTranche("pg_squeeze", NUM_WORKER_TASKS + 1);
}

void
squeeze_worker_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	workerData = ShmemInitStruct("pg_squeeze",
								 worker_shmem_size(),
								 &found);
	if (!found)
	{
		int			i;
		LWLockPadded *locks;

		locks = GetNamedLWLockTranche("pg_squeeze");

		for (i = 0; i < NUM_WORKER_TASKS; i++)
		{
			WorkerTask *task;

			task = &workerData->tasks[i];
			task->id = 0;
			task->dbid = InvalidOid;
			task->in_progress = false;
			task->lock = &locks->lock;
			locks++;
			ConditionVariableInit(&task->cv);
		}
		pg_atomic_init_u32(&workerData->next_task, 0);

		workerData->lock = &locks->lock;
		workerData->nslots = max_squeeze_workers();

		for (i = 0; i < workerData->nslots; i++)
		{
			WorkerSlot *slot = &workerData->slots[i];

			slot->dbid = InvalidOid;
			slot->relid = InvalidOid;
			SpinLockInit(&slot->progress.mutex);
			reset_progress(&slot->progress);
			slot->pid = InvalidPid;
			slot->latch = NULL;
		}
	}

	LWLockRelease(AddinShmemInitLock);
}

/* Mark this worker's slot unused. */
static void
worker_shmem_shutdown(int code, Datum arg)
{
	/* exiting before the slot was initialized? */
	if (MyWorkerSlot)
	{
		LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
		Assert(MyWorkerSlot->dbid != InvalidOid);
		MyWorkerSlot->dbid = InvalidOid;
		MyWorkerSlot->relid = InvalidOid;
		reset_progress(&MyWorkerSlot->progress);
		MyWorkerSlot->pid = InvalidPid;
		MyWorkerSlot->latch = NULL;
		LWLockRelease(workerData->lock);

		/* This shouldn't be necessary, but ... */
		MyWorkerSlot = NULL;
		MyWorkerProgress = NULL;
	}

	if (MyWorkerTask)
		release_task();
}

#if PG_VERSION_NUM >= 150000
extern Datum create_squeeze_worker_task(PG_FUNCTION_ARGS);
#endif

static void start_worker_internal(bool scheduler);

static void worker_sighup(SIGNAL_ARGS);
static void worker_sigterm(SIGNAL_ARGS);

static void scheduler_worker_loop(void);
static void squeeze_worker_loop(void);
static void process_tasks(MemoryContext task_cxt);

static void run_command(char *command, int rc);

/*
 * The function name is ..._worker instead of ..._workers for historic reasons
 * (originally there was only one worker). Is it worth changing?
 */
PG_FUNCTION_INFO_V1(squeeze_start_worker);
Datum
squeeze_start_worker(PG_FUNCTION_ARGS)
{
	int			i;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to start squeeze worker"))));

	start_worker_internal(true);

	for (i = 0; i < squeeze_workers_per_database; i++)
		start_worker_internal(false);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(squeeze_stop_worker);
Datum
squeeze_stop_worker(PG_FUNCTION_ARGS)
{
	int			i;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to stop squeeze worker"))));

	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		/*
		 * There should be at least two workers per database: one scheduler
		 * and one or more workers. Therefore we need to check all the slots
		 * even if a match is found.
		 */
		if (slot->dbid == MyDatabaseId)
			kill(slot->pid, SIGTERM);
	}
	LWLockRelease(workerData->lock);

	PG_RETURN_VOID();
}

/*
 * Wake up all the squeeze workers for given database.
 *
 * The return value tells whether at least one worker was sent the signal.
 */
static bool
wake_up_squeeze_workers(void)
{
	int			i;
	bool		found = false;

	/* Wake up all squeeze workers connected to this database. */
	LWLockAcquire(workerData->lock, LW_SHARED);
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		/*
		 * There should be at least two workers per database: one scheduler
		 * and one or more workers. Therefore we need to check all the slots
		 * even if a match is found.
		 */
		if (slot->dbid == MyDatabaseId && !slot->scheduler)
		{
			SetLatch(slot->latch);
			found = true;
		}
	}
	LWLockRelease(workerData->lock);

	if (!found)
	{
		char	   *dbname;
		bool		my_xact = false;

		if (!IsTransactionState())
		{
			StartTransactionCommand();
			my_xact = true;
		}

		dbname = get_database_name(MyDatabaseId);

		ereport(DEBUG1,
				(errmsg("no squeeze worker found for databse \"%s\"",
						dbname)));

		if (my_xact)
			CommitTransactionCommand();
	}

	return found;
}

/*
 * Submit a task for a squeeze worker and wait for its completion.
 *
 * This is a replacement for the squeeze_table() function so that pg_squeeze
 * >= 1.6 can still expose the functionality via the postgres executor.
 */
extern Datum squeeze_table_new(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(squeeze_table_new);
Datum
squeeze_table_new(PG_FUNCTION_ARGS)
{
	Name		relschema,
				relname;
	Name		indname = NULL;
	Name		tbspname = NULL;
	ArrayType  *ind_tbsps = NULL;
	WorkerTask *task;
	uint32		next_task_idx;
	int			task_id;
	bool		found;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 (errmsg("Both schema and table name must be specified"))));

	relschema = PG_GETARG_NAME(0);
	relname = PG_GETARG_NAME(1);
	if (!PG_ARGISNULL(2))
		indname = PG_GETARG_NAME(2);
	if (!PG_ARGISNULL(3))
		tbspname = PG_GETARG_NAME(3);
	if (!PG_ARGISNULL(4))
	{
		ind_tbsps = PG_GETARG_ARRAYTYPE_P(4);
		if (VARSIZE(ind_tbsps) >= IND_TABLESPACES_ARRAY_SIZE)
			ereport(ERROR,
					(errmsg("the value of \"ind_tablespaces\" is too big")));
	}

	/* Pick the task. */
	next_task_idx = pg_atomic_fetch_add_u32(&workerData->next_task, 1);
	task = &workerData->tasks[next_task_idx % NUM_WORKER_TASKS];

	/*
	 * Wait until there's no pending task.
	 *
	 * ConditionVariablePrepareToSleep() ensures that we don't miss the worker
	 * signal which might be sent before we call ConditionVariableSleep().
	 */
	ConditionVariablePrepareToSleep(&task->cv);
	while (true)
	{
		LWLockAcquire(task->lock, LW_EXCLUSIVE);
		if (task->dbid == InvalidOid)
			break;

		/*
		 * Another task is being processed (or still waiting for the worker).
		 */
		LWLockRelease(task->lock);

		/* Wait for a notification by the worker. */
		ConditionVariableSleep(&task->cv, PG_WAIT_EXTENSION);
	}
	ConditionVariableCancelSleep();

	/* Fill-in the task information. */
	task->dbid = MyDatabaseId;
	Assert(!task->in_progress);
	namestrcpy(&task->relschema, NameStr(*relschema));
	namestrcpy(&task->relname, NameStr(*relname));
	if (indname)
		namestrcpy(&task->indname, NameStr(*indname));
	else
		NameStr(task->indname)[0] = '\0';
	if (tbspname)
		namestrcpy(&task->tbspname, NameStr(*tbspname));
	else
		NameStr(task->tbspname)[0] = '\0';
	if (ind_tbsps)
		memcpy(task->ind_tbsps, ind_tbsps, VARSIZE(ind_tbsps));
	else
		SET_VARSIZE(task->ind_tbsps, 0);

	task_id = task->id;
	LWLockRelease(task->lock);

	found = wake_up_squeeze_workers();
	if (!found)
		start_worker_internal(false);

	/*
	 * Wait for the task to complete (again, no busy loop like above.)
	 *
	 * Regarding ConditionVariablePrepareToSleep(), see the comment above.
	 */
	ConditionVariablePrepareToSleep(&task->cv);
	while (true)
	{
		bool		done = false;

		LWLockAcquire(task->lock, LW_SHARED);

		/*
		 * Done if the task area is ready for (or being already used by)
		 * another task (submitted by another backend).
		 */
		if (task->id != task_id)
			done = true;
		LWLockRelease(task->lock);

		if (done)
			break;

		/*
		 * Processing of our task hasn't finished yet (maybe not even started)
		 * Wait for a notification by the worker.
		 */
		ConditionVariableSleep(&task->cv, PG_WAIT_EXTENSION);
	}
	ConditionVariableCancelSleep();

	/*
	 * Due to the waiting for the task completion we could have consumed the
	 * signal by the worker, which is also awaited by another backend to try
	 * submitting its task. Re-send that signal on behalf of the worker now.
	 */
	ConditionVariableSignal(&task->cv);

	PG_RETURN_VOID();
}

/*
 * Register either scheduler or squeeze worker, according to the argument.
 */
static void
start_worker_internal(bool scheduler)
{
	WorkerConInteractive con;
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	char	   *kind = scheduler ? "scheduler" : "squeeze";

	con.dbid = MyDatabaseId;
	con.roleid = GetUserId();
	con.scheduler = scheduler;
	squeeze_initialize_bgworker(&worker, NULL, &con, MyProcPid);

	ereport(DEBUG1, (errmsg("registering pg_squeeze %s worker", kind)));
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("More details may be available in the server log.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	ereport(DEBUG1,
			(errmsg("pg_squeeze %s worker started, pid=%d", kind, pid)));
}

/*
 * Convenience routine to allocate the structure in TopMemoryContext. We need
 * it to survive fork and initialization of the worker.
 *
 * (The allocation cannot be avoided as BackgroundWorker.bgw_extra does not
 * provide enough space for us.)
 */
WorkerConInit *
allocate_worker_con_info(char *dbname, char *rolename, bool scheduler)
{
	WorkerConInit *result;

	result = (WorkerConInit *) MemoryContextAllocZero(TopMemoryContext,
													  sizeof(WorkerConInit));
	result->dbname = MemoryContextStrdup(TopMemoryContext, dbname);
	result->rolename = MemoryContextStrdup(TopMemoryContext, rolename);
	result->scheduler = scheduler;
	return result;
}

/* Initialize the worker and pass connection info in the appropriate form. */
void
squeeze_initialize_bgworker(BackgroundWorker *worker,
							WorkerConInit *con_init,
							WorkerConInteractive *con_interactive,
							pid_t notify_pid)
{
	char	   *dbname;
	bool		scheduler;
	char	   *kind;

	worker->bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker->bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker->bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker->bgw_library_name, "pg_squeeze");
	sprintf(worker->bgw_function_name, "squeeze_worker_main");

	if (con_init != NULL)
	{
		worker->bgw_main_arg = (Datum) PointerGetDatum(con_init);
		dbname = con_init->dbname;
		scheduler = con_init->scheduler;
	}
	else if (con_interactive != NULL)
	{
		worker->bgw_main_arg = (Datum) 0;

		StaticAssertStmt(sizeof(WorkerConInteractive) <= BGW_EXTRALEN,
						 "WorkerConInteractive is too big");
		memcpy(worker->bgw_extra, con_interactive,
			   sizeof(WorkerConInteractive));

		/*
		 * Catalog lookup is possible during interactive start, so do it for
		 * the sake of bgw_name. Comment of WorkerConInteractive structure
		 * explains why we still must use the OID for worker registration.
		 */
		dbname = get_database_name(con_interactive->dbid);
		scheduler = con_interactive->scheduler;
	}
	else
		elog(ERROR, "Connection info not available for squeeze worker.");

	kind = scheduler ? "scheduler" : "squeeze";
	snprintf(worker->bgw_name, BGW_MAXLEN,
			 "pg_squeeze %s worker for database %s",
			 kind, dbname);
	snprintf(worker->bgw_type, BGW_MAXLEN, "squeeze worker");

	worker->bgw_notify_pid = notify_pid;
}

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/*
 * Sleep time (in seconds) of the scheduler worker.
 *
 * If there are no tables eligible for squeezing, the worker sleeps this
 * amount of seconds and then try again. The value should be low enough to
 * ensure that no scheduled table processing is missed, while the schedule
 * granularity is one minute.
 *
 * So far there seems to be no reason to have separate variables for the
 * scheduler and the squeeze worker.
 */
static int	worker_naptime = 20;

/*
 * There are 2 kinds of worker: 1) scheduler, which creates new tasks, 2) the
 * actual "squeeze worker" which calls the squeeze_table() function. With
 * scheduling independent from the actual table processing we check the table
 * status exactly (with the granularity of one minute) at the scheduled
 * time. This way we avoid the tricky question how long should particular
 * schedule stay valid and thus we can use equality operator to check if the
 * scheduled time is there.
 *
 * There are 2 alternatives to the equality test: 1) schedule is valid for
 * some time interval which is hard to define, 2) the schedule is valid
 * forever - this is bad because it allows table processing even hours after
 * the schedule if the table happens to get bloated some time after the
 * schedule.
 *
 * If the squeeze_table() function makes the following tasks delayed, it's
 * another problem that we can address by increasing the number of "squeeze
 * workers". (In that case we'd have to adjust the replication slot naming
 * scheme so that there are no conflicts.)
 */
static bool am_i_scheduler = false;

void
squeeze_worker_main(Datum main_arg)
{
	Datum		arg;
	int			i;
	bool		found;
	int			nworkers;

	pqsignal(SIGHUP, worker_sighup);
	pqsignal(SIGTERM, worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Retrieve connection info. */
	Assert(MyBgworkerEntry != NULL);
	arg = MyBgworkerEntry->bgw_main_arg;

	if (MyBgworkerEntry->bgw_main_arg != (Datum) 0)
	{
		WorkerConInit *con;

		con = (WorkerConInit *) DatumGetPointer(arg);
		am_i_scheduler = con->scheduler;
		BackgroundWorkerInitializeConnection(con->dbname, con->rolename, 0	/* flags */
			);
	}
	else
	{
		WorkerConInteractive con;

		/* Ensure aligned access. */
		memcpy(&con, MyBgworkerEntry->bgw_extra,
			   sizeof(WorkerConInteractive));
		am_i_scheduler = con.scheduler;
		BackgroundWorkerInitializeConnectionByOid(con.dbid, con.roleid, 0);
	}

	/* The worker should do its cleanup when exiting. */
	before_shmem_exit(worker_shmem_shutdown, (Datum) 0);

	/*
	 * Make sure that there is no more than one scheduler and no more than
	 * squeeze_workers_per_database workers running on this database.
	 */
	found = false;
	nworkers = 0;
	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		if (slot->dbid == MyDatabaseId)
		{
			if (am_i_scheduler && slot->scheduler)
			{
				elog(WARNING,
					 "one scheduler worker already running on database oid=%u",
					 MyDatabaseId);

				found = true;
				break;
			}
			else if (!am_i_scheduler && !slot->scheduler)
			{
				if (++nworkers >= squeeze_workers_per_database)
				{
					elog(WARNING,
						 "%d squeeze worker(s) already running on database oid=%u",
						 nworkers, MyDatabaseId);
					break;
				}
			}
		}
	}
	LWLockRelease(workerData->lock);
	if (found || (nworkers >= squeeze_workers_per_database))
		goto done;

	/* Find and initialize a slot for this worker. */
	Assert(MyWorkerSlot == NULL);
	found = false;
	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		if (slot->dbid == InvalidOid)
		{
			slot->dbid = MyDatabaseId;
			Assert(slot->relid == InvalidOid);
			Assert(slot->pid == InvalidPid);
			slot->pid = MyProcPid;
			slot->scheduler = am_i_scheduler;
			slot->latch = MyLatch;

			MyWorkerSlot = slot;
			MyWorkerProgress = &slot->progress;
			reset_progress(MyWorkerProgress);

			found = true;
			break;
		}
	}
	LWLockRelease(workerData->lock);
	if (!found)
	{
		/*
		 * This should never happen (i.e. we should always have
		 * max_worker_processes slots), but check, in case the slots leak.
		 * Furthermore, for PG < 15 the maximum number of workers is a compile
		 * time constant, so this is where we check the length of the slot
		 * array.
		 */
		elog(WARNING,
			 "no unused slot found for pg_squeeze worker process");

		goto done;
	}

	if (am_i_scheduler)
		scheduler_worker_loop();
	else
		squeeze_worker_loop();

done:
	proc_exit(0);
}

static void
worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
scheduler_worker_loop(void)
{
	long		delay = 0L;

	while (!got_sigterm)
	{
		int			rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, delay,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ereport(DEBUG1, (errmsg("scheduler worker: checking the schedule")));

		run_command("SELECT squeeze.check_schedule()", SPI_OK_SELECT);

		/*
		 * If worker is idle, there's no reason to let it wait until its
		 * naptime is over.
		 */
		wake_up_squeeze_workers();

		/* Check later if any table meets the schedule. */
		delay = worker_naptime * 1000L;
	}
}

static void
squeeze_worker_loop(void)
{
	long		delay = 0L;
	MemoryContext task_cxt;

	/* Memory context for auxiliary per-task allocations. */
	task_cxt = AllocSetContextCreate(TopMemoryContext,
									 "pg_squeeze task context",
									 ALLOCSET_DEFAULT_SIZES);

	while (!got_sigterm)
	{
		int			rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, delay,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Turn new tasks into ready (or processed if the tables should not
		 * really be squeezed). The scheduler worker should not do this
		 * because the checks can take some time.
		 */
		run_command("SELECT squeeze.dispatch_new_tasks()", SPI_OK_SELECT);

		/* Process the pending task(s). */
		MemoryContextReset(task_cxt);
		process_tasks(task_cxt);

		/*
		 * Release the replication slot explicitly, ERROR does not ensure
		 * that. (PostgresMain does that for regular backend in the main loop,
		 * but we don't let the error propagate there because we need to log
		 * it.)
		 */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();

		delay = worker_naptime * 1000L;
	}

	MemoryContextDelete(task_cxt);
}

/*
 * process_next_task() function used to be implemented in pl/pgsql. However,
 * since it calls the squeeze_table() function and since the commit 240e0dbacd
 * in PG core makes it impossible to call squeeze_table() via the postgres
 * executor, this function must be implemented in C and call squeeze_table()
 * directly.
 *
 * The name was also changed so it reflects the fact that multiple tasks can
 * be processed by a single call.
 */
static void
process_tasks(MemoryContext task_cxt)
{
	StringInfoData query;
	int			ret;
	Name		relschema,
				relname;
	Name		cl_index,
				rel_tbsp;
	ArrayType  *ind_tbsps;
	MemoryContext oldcxt;
	TaskDetails *tasks = NULL;
	uint64		ntasks;
	int			i;
	static int	next_task = 0;
	WorkerTask *task;

	initStringInfo(&query);

restart:
	cl_index = NULL;
	rel_tbsp = NULL;
	ind_tbsps = NULL;
	if (tasks)
	{
		for (i = 0; i < ntasks; i++)
		{
			TaskDetails *this = &tasks[i];

			if (this->ind_tbsps)
				pfree(this->ind_tbsps);
		}
		pfree(tasks);
	}
	tasks = NULL;
	ntasks = 0;
	resetStringInfo(&query);

	Assert(MyWorkerTask == NULL);

	/* First, check for a task in the shared memory. */
	for (i = 0; i < NUM_WORKER_TASKS; i++)
	{
		/*
		 * Start where we've ended last time. If we always started at 0, the
		 * tasks at lower positions would have higher chance to get a worker
		 * assigned than those at higher positions.
		 */
		next_task++;
		next_task %= NUM_WORKER_TASKS;
		task = &workerData->tasks[next_task];

		LWLockAcquire(task->lock, LW_SHARED);
		if (task->dbid == MyDatabaseId && !task->in_progress)
		{
			uint32		arr_size;

			relschema = &task->relschema;
			relname = &task->relname;
			if (strlen(NameStr(task->indname)) > 0)
				cl_index = &task->indname;
			if (strlen(NameStr(task->tbspname)) > 0)
				rel_tbsp = &task->tbspname;

			/*
			 * Create a single-element array of tasks for further processing.
			 */
			ntasks = 1;
			oldcxt = CurrentMemoryContext;
			MemoryContextSwitchTo(task_cxt);
			tasks = (TaskDetails *) palloc(sizeof(TaskDetails));

			/*
			 * Now that we're in the suitable memory context, we can copy the
			 * tablespace mapping array, if one is passed.
			 */
			arr_size = VARSIZE(task->ind_tbsps);
			if (arr_size > 0)
			{
				Assert(arr_size <= IND_TABLESPACES_ARRAY_SIZE);
				ind_tbsps = palloc(arr_size);
				memcpy(ind_tbsps, task->ind_tbsps, arr_size);
			}

			init_task_details(tasks, 0, relschema, relname, cl_index, rel_tbsp,
							  ind_tbsps, false, false);
			MemoryContextSwitchTo(oldcxt);

			/* No other worker should pick this task. */
			task->in_progress = true;

			MyWorkerTask = task;
		}
		LWLockRelease(task->lock);

		if (MyWorkerTask)
			break;
	}

	if (MyWorkerTask == NULL)
	{
		TupleDesc	tupdesc;
		TupleTableSlot *slot;

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Fetch this many tasks per query execution. */
#define TASK_BATCH_SIZE		4

		appendStringInfo(&query,
						 "SELECT tb.tabschema, tb.tabname, tb.clustering_index,\
tb.rel_tablespace, tb.ind_tablespaces, t.id, \
t.tried >= tb.max_retry, tb.skip_analyze \
FROM squeeze.tasks t, squeeze.tables tb \
WHERE t.table_id = tb.id AND t.state = 'ready' \
ORDER BY t.created \
LIMIT %d", TASK_BATCH_SIZE);

		if (SPI_connect() != SPI_OK_CONNECT)
			ereport(ERROR, (errmsg("could not connect to SPI manager")));
		pgstat_report_activity(STATE_RUNNING, query.data);
		ret = SPI_execute(query.data, true, 0);
		pgstat_report_activity(STATE_IDLE, NULL);

		if (ret != SPI_OK_SELECT)
			ereport(ERROR, (errmsg("SELECT command failed: %s", query.data)));

#if PG_VERSION_NUM >= 130000
		ntasks = SPI_tuptable->numvals;
#else
		ntasks = SPI_processed;
#endif
		if (ntasks == 0)
		{
			if (SPI_finish() != SPI_OK_FINISH)
				ereport(ERROR, (errmsg("SPI_finish failed")));

			PopActiveSnapshot();
			CommitTransactionCommand();
			return;
		}

		Assert(ntasks <= TASK_BATCH_SIZE);

		/*
		 * Create the tuple descriptor, slot and the task details in a
		 * separate memory context, so that it survives the SPI session.
		 */
		oldcxt = CurrentMemoryContext;
		MemoryContextSwitchTo(task_cxt);
		tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
#if PG_VERSION_NUM >= 120000
		slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
#else
		slot = MakeSingleTupleTableSlot(tupdesc);
#endif
		tasks = (TaskDetails *) palloc(ntasks * sizeof(TaskDetails));
		for (i = 0; i < ntasks; i++)
		{
			HeapTuple	tup;
			Datum		datum;
			bool		isnull;
			int32		task_id;
			bool		last_try,
						skip_analyze;

			cl_index = NULL;
			rel_tbsp = NULL;
			ind_tbsps = NULL;

			/* Retrieve the tuple attributes. */
			tup = heap_copytuple(SPI_tuptable->vals[i]);
			ExecClearTuple(slot);
#if PG_VERSION_NUM >= 120000
			ExecStoreHeapTuple(tup, slot, true);
#else
			ExecStoreTuple(tup, slot, InvalidBuffer, true);
#endif

			datum = slot_getattr(slot, 1, &isnull);
			Assert(!isnull);
			relschema = DatumGetName(datum);

			datum = slot_getattr(slot, 2, &isnull);
			Assert(!isnull);
			relname = DatumGetName(datum);

			datum = slot_getattr(slot, 3, &isnull);
			if (!isnull)
				cl_index = DatumGetName(datum);

			datum = slot_getattr(slot, 4, &isnull);
			if (!isnull)
				rel_tbsp = DatumGetName(datum);

			datum = slot_getattr(slot, 5, &isnull);
			if (!isnull)
				ind_tbsps = DatumGetArrayTypePCopy(datum);

			datum = slot_getattr(slot, 6, &isnull);
			Assert(!isnull);
			task_id = DatumGetInt32(datum);

			datum = slot_getattr(slot, 7, &isnull);
			Assert(!isnull);
			last_try = DatumGetBool(datum);

			datum = slot_getattr(slot, 8, &isnull);
			Assert(!isnull);
			skip_analyze = DatumGetBool(datum);

			init_task_details(&tasks[i], task_id, relschema, relname,
							  cl_index, rel_tbsp, ind_tbsps, last_try,
							  skip_analyze);

		}
		MemoryContextSwitchTo(oldcxt);
		ExecDropSingleTupleTableSlot(slot);
		FreeTupleDesc(tupdesc);

		/* Finish the data retrieval. */
		if (SPI_finish() != SPI_OK_FINISH)
			ereport(ERROR, (errmsg("SPI_finish failed")));
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	/* Now process the tasks. */
	for (i = 0; i < ntasks; i++)
	{
		TimestampTz start_ts;
		bool		success;
		TaskDetails *td = &tasks[i];
		RangeVar   *relrv;
		Relation	rel;
		Oid			relid;
		int			j;
		bool		found;
		ErrorData  *edata;

		ereport(DEBUG1,
				(errmsg("task for table %s.%s is ready for processing",
						NameStr(td->relschema), NameStr(td->relname))));

		cl_index = NULL;
		rel_tbsp = NULL;

		/* Check if another worker is already processing this table. */
		StartTransactionCommand();
		relrv = makeRangeVar(NameStr(td->relschema), NameStr(td->relname), -1);
		success = true;
		PG_TRY();
		{
#if PG_VERSION_NUM >= 120000
			rel = table_openrv(relrv, AccessShareLock);
			relid = RelationGetRelid(rel);
			table_close(rel, AccessShareLock);
#else
			rel = heap_openrv(relrv, AccessShareLock);
			relid = RelationGetRelid(rel);
			heap_close(rel, AccessShareLock);
#endif
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			squeeze_handle_error_db(&edata, task_cxt);
			squeeze_handle_error_app(edata, td);
			success = false;
		}
		PG_END_TRY();

		if (!success)
		{
			if (MyWorkerTask)
				release_task();
			continue;
		}

		LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
		found = false;
		for (j = 0; j < workerData->nslots; j++)
		{
			WorkerSlot *slot = &workerData->slots[j];

			if (slot->dbid == MyDatabaseId &&
				slot->relid == relid)
			{
				found = true;
				break;
			}
		}
		if (found)
		{
			/* XXX Print out the PID of the other worker? */
			ereport(DEBUG1,
					(errmsg("task for table %s.%s is being processed by another worker",
							NameStr(td->relschema), NameStr(td->relname))));
			LWLockRelease(workerData->lock);

			if (MyWorkerTask)
			{
				Assert(ntasks == 1);

				release_task();
			}

			continue;
		}
		/* Declare that this worker takes care of the relation. */
		Assert(MyWorkerSlot->dbid == MyDatabaseId);
		MyWorkerSlot->relid = relid;
		reset_progress(&MyWorkerSlot->progress);
		LWLockRelease(workerData->lock);

		/*
		 * In theory, the table can be dropped now, created again (with a
		 * different OID), scheduled for processing and picked by another
		 * worker. Concurrent processing of the same table should make
		 * squeeze_table() fail because both (all) the backends will try to
		 * create a replication slot of the same name. It doesn't make sense
		 * to spend more effort on protection against such a rare conditions
		 * here.
		 */

		if (strlen(NameStr(td->cl_index)) > 0)
			cl_index = &td->cl_index;
		if (strlen(NameStr(td->rel_tbsp)) > 0)
			rel_tbsp = &td->rel_tbsp;

		/* Perform the actual work. */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		start_ts = GetCurrentTimestamp();
		success = squeeze_table_impl(&td->relschema, &td->relname, cl_index,
									 rel_tbsp, td->ind_tbsps, &edata, task_cxt);

		if (success)
			CommitTransactionCommand();
		else
		{
			/*
			 * The transaction should be aborted by squeeze_table_impl().
			 */
			squeeze_handle_error_app(edata, td);

			if (MyWorkerTask == NULL)
			{
				/*
				 * Some of the next tasks we have in the 'tasks' array could
				 * have been already processed by other workers. Those should
				 * have the state set to 'processed' (before the other slot
				 * could set MyWorkerSlot->relid to InvalidOid), so we just
				 * need to run the query at the top of this function again.
				 */
				goto restart;
			}
		}

		/* Insert an entry into the "squeeze.log" table. */
		if (success)
		{
			Oid			outfunc;
			bool		isvarlena;
			FmgrInfo	fmgrinfo;
			char	   *start_ts_str;

			StartTransactionCommand();
			getTypeOutputInfo(TIMESTAMPTZOID, &outfunc, &isvarlena);
			fmgr_info(outfunc, &fmgrinfo);
			start_ts_str = OutputFunctionCall(&fmgrinfo, start_ts);
			/* Make sure the string survives TopTransactionContext. */
			MemoryContextSwitchTo(task_cxt);
			start_ts_str = pstrdup(start_ts_str);
			MemoryContextSwitchTo(oldcxt);
			CommitTransactionCommand();

			resetStringInfo(&query);
			/*
			 * No one should change the progress fields now, so we can access
			 * them w/o the spinlock below.
			 */
			appendStringInfo(&query,
							 "INSERT INTO squeeze.log(tabschema, tabname, started, finished, ins_initial, ins, upd, del) \
VALUES ('%s', '%s', '%s', clock_timestamp(), %ld, %ld, %ld, %ld)",
							 NameStr(td->relschema),
							 NameStr(td->relname),
							 start_ts_str,
							 MyWorkerProgress->ins_initial,
							 MyWorkerProgress->ins,
							 MyWorkerProgress->upd,
							 MyWorkerProgress->del);
			run_command(query.data, SPI_OK_INSERT);

			if (MyWorkerTask == NULL)
			{
				/* Finalize the task. */
				resetStringInfo(&query);
				appendStringInfo(&query, "SELECT squeeze.finalize_task(%d)", td->id);
				run_command(query.data, SPI_OK_SELECT);

				if (!td->skip_analyze)
				{
					/*
					 * Analyze the new table, unless user rejects it
					 * explicitly.
					 *
					 * XXX Besides updating planner statistics in general,
					 * this sets pg_class(relallvisible) to 0, so that planner
					 * is not too optimistic about this figure. The
					 * preferrable solution would be to run (lazy) VACUUM
					 * (with the ANALYZE option) to initialize visibility map.
					 * However, to make the effort worthwile, we shouldn't do
					 * it until all transactions can see all the changes done
					 * by squeeze_table() function. What's the most suitable
					 * way to wait?  Asynchronous execution of the VACUUM is
					 * probably needed in any case.
					 */
					resetStringInfo(&query);
					appendStringInfo(&query, "ANALYZE %s.%s",
									 NameStr(td->relschema),
									 NameStr(td->relname));
					run_command(query.data, SPI_OK_UTILITY);
				}
			}
		}

		if (MyWorkerTask)
			release_task();

		/* Clear the relid field of this worker's slot. */
		LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
		MyWorkerSlot->relid = InvalidOid;
		reset_progress(&MyWorkerSlot->progress);
		LWLockRelease(workerData->lock);

		/* See above */
		goto restart;
	}
}

/*
 * Handle an error from the perspective of pg_squeeze
 *
 * Here we are especially interested in errors like incorrect user input
 * (e.g. non-existing table specified) or expiration of the
 * squeeze_max_xlock_time parameter. If the squeezing succeeded, the following
 * operations should succeed too, unless there's a bug in the extension - in
 * such a case it's o.k. to let the ERROR stop the worker.
 */
static void
squeeze_handle_error_app(ErrorData *edata, TaskDetails *td)
{
	StringInfoData query;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO squeeze.errors(tabschema, tabname, sql_state, err_msg, err_detail) \
VALUES ('%s', '%s', '%s', '%s', '%s')",
					 NameStr(td->relschema),
					 NameStr(td->relname),
					 unpack_sql_state(edata->sqlerrcode),
					 edata->message,
					 edata->detail ? edata->detail : "");
	run_command(query.data, SPI_OK_INSERT);

	if (MyWorkerTask == NULL)
	{
		/* If the active task failed too many times, cancel it. */
		resetStringInfo(&query);
		if (td->last_try)
		{
			appendStringInfo(&query,
							 "SELECT squeeze.cancel_task(%d)",
							 td->id);
			run_command(query.data, SPI_OK_SELECT);
		}
		else
		{
			/* Account for the current attempt. */
			appendStringInfo(&query,
							 "UPDATE squeeze.tasks SET tried = tried + 1 WHERE id = %d",
							 td->id);
			run_command(query.data, SPI_OK_UPDATE);
		}

		/* Clear the relid field of this worker's slot. */
		LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
		MyWorkerSlot->relid = InvalidOid;
		reset_progress(&MyWorkerSlot->progress);
		LWLockRelease(workerData->lock);
	}
}

static void
release_task(void)
{
	/* Make room for the next task. */
	LWLockAcquire(MyWorkerTask->lock, LW_EXCLUSIVE);
	MyWorkerTask->dbid = InvalidOid;
	MyWorkerTask->in_progress = false;
	MyWorkerTask->id++;
	LWLockRelease(MyWorkerTask->lock);

	/* Notify the next backend that is trying to create a new task. */
	ConditionVariableSignal(&MyWorkerTask->cv);

	MyWorkerTask = NULL;
}

static void
reset_progress(WorkerProgress *progress)
{
	SpinLockAcquire(&progress->mutex);
	progress->ins_initial = 0;
	progress->ins = 0;
	progress->upd = 0;
	progress->del = 0;
	SpinLockRelease(&progress->mutex);
}

/*
 * Run an SQL command that does not return any value.
 */
static void
run_command(char *command, int rc)
{
	int			ret;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, command);

	ret = SPI_execute(command, false, 0);
	if (ret != rc)
		elog(ERROR, "command failed: %s", command);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}

static void
init_task_details(TaskDetails *task, int32 task_id, Name relschema,
				  Name relname, Name cl_index, Name rel_tbsp,
				  ArrayType *ind_tbsps, bool last_try, bool skip_analyze)
{
	memset(task, 0, sizeof(TaskDetails));
	task->id = task_id;
	namestrcpy(&task->relschema, NameStr(*relschema));
	namestrcpy(&task->relname, NameStr(*relname));
	if (cl_index)
		namestrcpy(&task->cl_index, NameStr(*cl_index));
	if (rel_tbsp)
		namestrcpy(&task->rel_tbsp, NameStr(*rel_tbsp));
	task->ind_tbsps = ind_tbsps;
	task->last_try = last_try;
	task->skip_analyze = skip_analyze;
}

#define	ACTIVE_WORKERS_RES_ATTRS	7

/* Get information on squeeze workers on the current database. */
PG_FUNCTION_INFO_V1(squeeze_get_active_workers);
Datum
squeeze_get_active_workers(PG_FUNCTION_ARGS)
{
	WorkerSlot *slots,
			   *dst;
	int			i,
				nslots = 0;
#if PG_VERSION_NUM >= 150000
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);
#else
	FuncCallContext *funcctx;
	int			call_cntr,
				max_calls;
	HeapTuple  *tuples;
#endif

	/*
	 * Copy the slots information so that we don't have to keep the slot array
	 * locked for longer time than necessary.
	 */
	slots = (WorkerSlot *) palloc(workerData->nslots * sizeof(WorkerSlot));
	dst = slots;
	LWLockAcquire(workerData->lock, LW_SHARED);
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		if (!slot->scheduler &&
			slot->pid != InvalidPid &&
			slot->dbid == MyDatabaseId)
		{
			memcpy(dst, slot, sizeof(WorkerSlot));
			dst++;
			nslots++;
		}
	}
	LWLockRelease(workerData->lock);

#if PG_VERSION_NUM >= 150000
	for (i = 0; i < nslots; i++)
	{
		WorkerSlot *slot = &slots[i];
		WorkerProgress *progress = &slot->progress;
		Datum		values[ACTIVE_WORKERS_RES_ATTRS];
		bool		isnull[ACTIVE_WORKERS_RES_ATTRS];
		char	   *relnspc = NULL;
		char	   *relname = NULL;
		NameData	tabname,
					tabschema;

		memset(isnull, false, ACTIVE_WORKERS_RES_ATTRS * sizeof(bool));
		values[0] = Int32GetDatum(slot->pid);

		if (OidIsValid(slot->relid))
		{
			Oid			nspid;

			/*
			 * It's possible that processing of the relation has finished and
			 * the relation (or even the namespace) was dropped. Therefore,
			 * stop catalog lookups as soon as any object is missing. XXX
			 * Furthermore, the relid can already be in use by another
			 * relation, but that's very unlikely, not worth special effort.
			 */
			nspid = get_rel_namespace(slot->relid);
			if (OidIsValid(nspid))
				relnspc = get_namespace_name(nspid);
			if (relnspc)
				relname = get_rel_name(slot->relid);
		}
		if (relnspc == NULL || relname == NULL)
			continue;

		namestrcpy(&tabschema, relnspc);
		values[1] = NameGetDatum(&tabschema);
		namestrcpy(&tabname, relname);
		values[2] = NameGetDatum(&tabname);
		values[3] = Int64GetDatum(progress->ins_initial);
		values[4] = Int64GetDatum(progress->ins);
		values[5] = Int64GetDatum(progress->upd);
		values[6] = Int64GetDatum(progress->del);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, isnull);
	}

	return (Datum) 0;
#else
	/* Less trivial implementation, to be removed when PG 14 is EOL. */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			ntuples = 0;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
		/* XXX Is this necessary? */
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* Process only the slots that we really can display. */
		tuples = (HeapTuple *) palloc0(nslots * sizeof(HeapTuple));
		for (i = 0; i < nslots; i++)
		{
			WorkerSlot *slot = &slots[i];
			WorkerProgress *progress = &slot->progress;
			char	   *relnspc = NULL;
			char	   *relname = NULL;
			NameData	tabname,
						tabschema;
			Datum	   *values;
			bool	   *isnull;

			values = (Datum *) palloc(ACTIVE_WORKERS_RES_ATTRS * sizeof(Datum));
			isnull = (bool *) palloc0(ACTIVE_WORKERS_RES_ATTRS * sizeof(bool));

			if (OidIsValid(slot->relid))
			{
				Oid			nspid;

				/* See the PG 15 implementation above. */
				nspid = get_rel_namespace(slot->relid);
				if (OidIsValid(nspid))
					relnspc = get_namespace_name(nspid);
				if (relnspc)
					relname = get_rel_name(slot->relid);
			}
			if (relnspc == NULL || relname == NULL)
				continue;

			values[0] = Int32GetDatum(slot->pid);
			namestrcpy(&tabschema, relnspc);
			values[1] = NameGetDatum(&tabschema);
			namestrcpy(&tabname, relname);
			values[2] = NameGetDatum(&tabname);
			values[3] = Int64GetDatum(progress->ins_initial);
			values[4] = Int64GetDatum(progress->ins);
			values[5] = Int64GetDatum(progress->upd);
			values[6] = Int64GetDatum(progress->del);

			tuples[ntuples++] = heap_form_tuple(tupdesc, values, isnull);
		}
		funcctx->user_fctx = tuples;
		funcctx->max_calls = ntuples;;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	tuples = (HeapTuple *) funcctx->user_fctx;

	if (call_cntr < max_calls)
	{
		HeapTuple	tuple = tuples[call_cntr];
		Datum		result;

		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
#endif
}
