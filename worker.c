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
 * The maximum number of tasks submitted by the scheduler worker or by the
 * squeeze_table() user function that can be in progress at a time (as long as
 * there's enough workers). Note that this is cluster-wide constant.
 *
 * XXX Should be based on MAX_SQUEEZE_WORKERS? Not sure how to incorporate
 * scheduler workers in the computation.
 */
#define	NUM_WORKER_TASKS	16

typedef struct WorkerData
{
	WorkerTask	tasks[NUM_WORKER_TASKS];

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
WorkerTask *MyWorkerTask = NULL;

/* Local pointer to the progress information. */
WorkerProgress *MyWorkerProgress = NULL;

/*
 * The "squeeze worker" (i.e. one that performs the actual squeezing, as
 * opposed to the "scheduler worker"). The scheduler worker uses this
 * structure to keep track of squeeze workers it launched.
 */
typedef struct SqueezeWorker
{
	BackgroundWorkerHandle	*handle;
	WorkerTask	*task;
} SqueezeWorker;

static SqueezeWorker	*SqueezeWorkers = NULL;

static void interrupt_worker(WorkerTask *task);
static void release_task(WorkerTask *task, bool worker);
static void reset_progress(WorkerProgress *progress);
static void squeeze_handle_error_app(ErrorData *edata, WorkerTask *task);

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
	RequestNamedLWLockTranche("pg_squeeze", 1);
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
			task->assigned = false;
			task->exit_requested = false;
			task->slot = NULL;
			SpinLockInit(&task->mutex);
		}

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
		release_task(MyWorkerTask, true);

	/*
	 * The scheduler worker needs to clean up the task it launched, so they do
	 * not leak Besides that, it should terminate all the squeeze workers it
	 * launched, otherwise it'd be hard to keep track of the number of running
	 * workers if it's started again before the existing workers finish.
	 */
	if (SqueezeWorkers)
	{
		int		i;

		for (i = 0; i < squeeze_workers_per_database; i++)
		{
			SqueezeWorker	*worker = &SqueezeWorkers[i];

			if (worker->task)
			{
				interrupt_worker(worker->task);
				release_task(worker->task, false);
			}
		}
	}
}

#if PG_VERSION_NUM >= 150000
extern Datum create_squeeze_worker_task(PG_FUNCTION_ARGS);
#endif

static int get_unused_task_slot_id(int task_id);
static int count_workers(void);
static void fill_worker_task(WorkerTask *task, int id, Name relschema,
							 Name relname, Name indname, Name tbspname,
							 ArrayType *ind_tbsps, bool last_try,
							 bool skip_analyze);
static bool start_worker_internal(bool scheduler, int task_id,
								  BackgroundWorkerHandle **handle);

static void worker_sighup(SIGNAL_ARGS);
static void worker_sigterm(SIGNAL_ARGS);

static void scheduler_worker_loop(void);
static void squeeze_worker_loop(int task_id);
static void process_task(int task_id, MemoryContext task_cxt);

static uint64 run_command(char *command, int rc);

/*
 * Start the scheduler worker.
 */
PG_FUNCTION_INFO_V1(squeeze_start_worker);
Datum
squeeze_start_worker(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to start squeeze worker"))));

	start_worker_internal(true, -1, NULL);

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
	int		task_id;
	WorkerTask *task = NULL;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;

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

	/* Find free task structure. */
	task_id = get_unused_task_slot_id(-1);
	if (task_id < 0)
		ereport(ERROR, (errmsg("too many concurrent tasks in progress")));

	/* Fill-in the remaining task information. */
	task = &workerData->tasks[task_id];
	fill_worker_task(task, -1, relschema, relname, indname, tbspname,
					 ind_tbsps, false, true);

	/* Start the worker to handle the task. */
	PG_TRY();
	{
		if (!start_worker_internal(false, task_id, &handle))
			ereport(ERROR,
					(errmsg("a new worker could not start due to squeeze.workers_per_database")));
	}
	PG_CATCH();
	{
		/*
		 * It seems possible that the worker is trying to start even if we end
		 * up here - at least when WaitForBackgroundWorkerStartup() got
		 * interrupted.
		 */
		interrupt_worker(task);

		release_task(task, false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Wait for the worker's exit. */
	PG_TRY();
	{
		status = WaitForBackgroundWorkerShutdown(handle);
	}
	PG_CATCH();
	{
		/*
		 * Make sure the worker stops. Interrupt received from the user is the
		 * typical use case.
		 */
		interrupt_worker(task);

		release_task(task, false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(ERROR,
				(errmsg("the postmaster died before the background worker could finish"),
				 errhint("More details may be available in the server log.")));
		/* No need to release the task in the shared memory. */
	}

	/*
	 * WaitForBackgroundWorkerShutdown() should not return anything else.
	 */
	Assert(status == BGWH_STOPPED);

	release_task(task, false);

	PG_RETURN_VOID();
}

/*
 * Returns a zero-based index of an unused task slot in the shared memory, or
 * -1 if there's no unused slot.
 *
 * If task_id is >= 0, avoid fetching tasks whose task_id equals to this
 * argument.
 */
static int
get_unused_task_slot_id(int task_id)
{
	int		i;
	WorkerTask	*task;
	int		res = -1;

	for (i = 0; i < NUM_WORKER_TASKS; i++)
	{
		int		this_task_id;

		task = &workerData->tasks[i];
		SpinLockAcquire(&task->mutex);
		/*
		 * If slot is valid, the worker is still working on an earlier task,
		 * although the backend that assigned the task already exited.
		 */
		if (!task->assigned && task->slot == NULL)
		{
			/* Make sure that no other backend can use the task. */
			task->assigned = true;
			res = i;
		}
		this_task_id = task->task_id;
		SpinLockRelease(&task->mutex);

		/*
		 * If task_id is valid, we check below if another task has the same
		 * task_id.
		 */
		if (res >= 0 && task_id < 0)
			break;

		if (task_id > 0 && task_id == this_task_id)
		{
			/* Undo the assignment if made one above. */
			if (res >= 0)
			{
				task = &workerData->tasks[res];
				SpinLockAcquire(&task->mutex);
				task->assigned = false;
				SpinLockRelease(&task->mutex);
				res = -1;
			}

			/* No point in searching further. */
			break;
		}
	}

	return res;
}

/*
 * Count active workers running in the current database.
 */
static int
count_workers(void)
{
	int		i;
	int		nworkers = 0;

	LWLockAcquire(workerData->lock, LW_SHARED);
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		if (slot->dbid == MyDatabaseId && !slot->scheduler)
			nworkers++;
	}
	LWLockRelease(workerData->lock);

	return nworkers;
}

/*
 * Fill-in "user data" of WorkerTask.
 */
static void
fill_worker_task(WorkerTask *task, int id, Name relschema, Name relname,
				 Name indname, Name tbspname, ArrayType *ind_tbsps,
				 bool last_try, bool skip_analyze)
{
	task->task_id = id;
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
	{
		if (VARSIZE(ind_tbsps) > IND_TABLESPACES_ARRAY_SIZE)
			ereport(ERROR, (errmsg("the array of index tablespaces is too big")));
		memcpy(task->ind_tbsps, ind_tbsps, VARSIZE(ind_tbsps));
	}
	else
		SET_VARSIZE(task->ind_tbsps, 0);

	task->last_try = last_try;
	task->skip_analyze = skip_analyze;

}

/*
 * Register either scheduler or squeeze worker, according to the argument.
 *
 * The number of scheduler workers per database is limited by the
 * squeeze_workers_per_database configuration variable.
 *
 * If task_id is >=0, then it's an index into the array of WorkerTask's in the
 * shared memory. In such a case, wait for the worker to start.
 *
 * Returns false if the worker could not be started due to
 * squeeze_workers_per_database, true in other cases.
 */
static bool
start_worker_internal(bool scheduler, int task_id, BackgroundWorkerHandle **handle)
{
	WorkerConInteractive con;
	BackgroundWorker worker;
	char	   *kind;
	BgwHandleStatus status;
	pid_t		pid;

	Assert(!scheduler || task_id < 0);

	/* Count the squeeze workers. */
	if (!scheduler)
	{
		int		nworkers = count_workers();

		if (nworkers >= squeeze_workers_per_database)
		{
			ereport(DEBUG1,
					(errmsg("cannot start a new squeeze worker, %d workers already running for database %u",
							squeeze_workers_per_database, MyDatabaseId)));
			return false;
		}
	}

	kind = scheduler ? "scheduler" : "squeeze";

	con.dbid = MyDatabaseId;
	con.roleid = GetUserId();
	con.scheduler = scheduler;
	con.task_id = task_id;
	squeeze_initialize_bgworker(&worker, NULL, &con, MyProcPid);

	ereport(DEBUG1, (errmsg("registering pg_squeeze %s worker", kind)));
	if (!RegisterDynamicBackgroundWorker(&worker, handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("More details may be available in the server log.")));

	if (task_id < 0)
		return true;

	if (handle == NULL)
		return true;

	status = WaitForBackgroundWorkerStartup(*handle, &pid);
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
	/*
	 * WaitForBackgroundWorkerStartup() should not return
	 * BGWH_NOT_YET_STARTED.
	 */
	Assert(status == BGWH_STARTED);

	ereport(DEBUG1,
			(errmsg("pg_squeeze %s worker started, pid=%d", kind, pid)));

	return true;
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
	int			task_id = -1;

	/* The worker should do its cleanup when exiting. */
	before_shmem_exit(worker_shmem_shutdown, (Datum) 0);

	pqsignal(SIGHUP, worker_sighup);
	pqsignal(SIGTERM, worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Retrieve connection info. */
	Assert(MyBgworkerEntry != NULL);
	arg = MyBgworkerEntry->bgw_main_arg;

	if (arg != (Datum) 0)
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

		task_id = con.task_id;
	}

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
				if (nworkers++ >= squeeze_workers_per_database)
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
		squeeze_worker_loop(task_id);

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
	int		i;
	SqueezeWorker	*worker;
	MemoryContext	sched_cxt, old_cxt;

	/* Initialize the array to track the workers we start. */
	SqueezeWorkers = (SqueezeWorker *) palloc(squeeze_workers_per_database *
											  sizeof(SqueezeWorker));
	for (i = 0; i < squeeze_workers_per_database; i++)
	{
		worker = &SqueezeWorkers[i];
		worker->handle = NULL;
		worker->task = NULL;
	}

	/* Context for allocations which cannot be freed too early. */
	sched_cxt = AllocSetContextCreate(TopMemoryContext,
									  "pg_squeeze scheduler context",
									  ALLOCSET_DEFAULT_SIZES);

	while (!got_sigterm)
	{
		int		navail = 0;
		StringInfoData	query;
		int			rc;
		uint64		ntask;
		TupleDesc	tupdesc;
		TupleTableSlot *slot;
		List	*ids = NIL;
		ListCell	*lc;

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
		 * Check the status of the workers we have started in the previous
		 * loops, and see how many workers we can launch in this loop.
		 */
		for (i = 0; i < squeeze_workers_per_database; i++)
		{
			worker = &SqueezeWorkers[i];

			if (worker->handle)
			{
				pid_t		pid;
				BgwHandleStatus	status;

				status = GetBackgroundWorkerPid(worker->handle, &pid);

				if (status == BGWH_STOPPED)
				{
					worker->handle = NULL;
					release_task(worker->task, false);
					worker->task = NULL;
				}
				else if (status == BGWH_POSTMASTER_DIED)
				{
					ereport(ERROR,
							(errmsg("the postmaster died before the squeeze worker could finish"),
							 errhint("More details may be available in the server log.")));
				}

			}
			if (worker->handle == NULL)
				navail++;
		}

		/*
		 * If we cannot start a new worker, wait on the latch until we're
		 * notified about a status change of a worker we launched earlier.
		 */
		if (navail == 0)
			continue;

		ereport(DEBUG1, (errmsg("scheduler worker: %d workers available",
								navail)));

		run_command("SELECT squeeze.check_schedule()", SPI_OK_SELECT);

		/*
		 * Turn new tasks into ready (or processed if the tables should not
		 * really be squeezed).
		 */
		run_command("SELECT squeeze.dispatch_new_tasks()", SPI_OK_SELECT);

		/*
		 * Are there some tasks with no worker assigned?
		 */
		initStringInfo(&query);
		appendStringInfo(
			&query,
			"SELECT t.id, tb.tabschema, tb.tabname, tb.clustering_index, "
			"tb.rel_tablespace, tb.ind_tablespaces, t.tried >= tb.max_retry, "
			"tb.skip_analyze "
			"FROM squeeze.tasks t, squeeze.tables tb "
			"LEFT JOIN squeeze.get_active_workers() AS w "
			"ON (tb.tabschema, tb.tabname) = (w.tabschema, w.tabname) "
			"WHERE w.tabname ISNULL AND t.state = 'ready' AND t.table_id = tb.id "
			"ORDER BY t.id "
			"LIMIT %d", navail);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
			ereport(ERROR, (errmsg("could not connect to SPI manager")));
		pgstat_report_activity(STATE_RUNNING, query.data);
		rc = SPI_execute(query.data, true, 0);
		pgstat_report_activity(STATE_IDLE, NULL);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR, (errmsg("SELECT command failed: %s", query.data)));

#if PG_VERSION_NUM >= 130000
		ntask = SPI_tuptable->numvals;
#else
		ntask = SPI_processed;
#endif

		if (ntask > 0)
		{
			tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
#if PG_VERSION_NUM >= 120000
			slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
#else
			slot = MakeSingleTupleTableSlot(tupdesc);
#endif
		}

		/* Initialize the task slots. */
		ids = NIL;
		for (i = 0; i < ntask; i++)
		{
			int		id, task_id;
			WorkerTask	*task;
			HeapTuple	tup;
			Datum		datum;
			bool		isnull;
			Name	relschema, relname, cl_index, rel_tbsp;
			ArrayType *ind_tbsps;
			bool		last_try;
			bool		skip_analyze;

			cl_index = NULL;
			rel_tbsp = NULL;
			ind_tbsps = NULL;

			/* Retrieve the tuple attributes and use them to fill the task. */
			tup = heap_copytuple(SPI_tuptable->vals[i]);
			ExecClearTuple(slot);
#if PG_VERSION_NUM >= 120000
			ExecStoreHeapTuple(tup, slot, true);
#else
			ExecStoreTuple(tup, slot, InvalidBuffer, true);
#endif

			datum = slot_getattr(slot, 1, &isnull);
			Assert(!isnull);
			task_id = DatumGetInt32(datum);

			/*
			 * No point in fetching the remaining columns if all the tasks are
			 * already used.
			 */
			id = get_unused_task_slot_id(task_id);
			if (id < 0)
				break;
			task = &workerData->tasks[id];

			datum = slot_getattr(slot, 2, &isnull);
			Assert(!isnull);
			relschema = DatumGetName(datum);

			datum = slot_getattr(slot, 3, &isnull);
			Assert(!isnull);
			relname = DatumGetName(datum);

			datum = slot_getattr(slot, 4, &isnull);
			if (!isnull)
				cl_index = DatumGetName(datum);

			datum = slot_getattr(slot, 5, &isnull);
			if (!isnull)
				rel_tbsp = DatumGetName(datum);

			datum = slot_getattr(slot, 6, &isnull);
			if (!isnull)
				ind_tbsps = DatumGetArrayTypePCopy(datum);

			datum = slot_getattr(slot, 7, &isnull);
			Assert(!isnull);
			last_try = DatumGetBool(datum);

			datum = slot_getattr(slot, 8, &isnull);
			Assert(!isnull);
			skip_analyze = DatumGetBool(datum);

			/* Fill the task. */
			fill_worker_task(task, task_id, relschema, relname, cl_index,
							 rel_tbsp, ind_tbsps, last_try, skip_analyze);


			/* The list must survive SPI_finish(). */
			old_cxt = MemoryContextSwitchTo(sched_cxt);
			ids = lappend_int(ids, id);
			MemoryContextSwitchTo(old_cxt);
		}

		if (ntask > 0)
		{
			ExecDropSingleTupleTableSlot(slot);
			FreeTupleDesc(tupdesc);
		}

		/* Finish the data retrieval. */
		if (SPI_finish() != SPI_OK_FINISH)
			ereport(ERROR, (errmsg("SPI_finish failed")));
		PopActiveSnapshot();
		CommitTransactionCommand();

		/*
		 * Now that the transaction has committed, we can start the
		 * workers. (start_worker_internal() needs to run in a transaction
		 * because it does access the system catalog.)
		 */
		foreach(lc, ids)
		{
			SqueezeWorker	*worker = NULL;
			bool	found = false;
			int	task_id;
			bool	too_many;

			/* Find the array element for the next worker. */
			for (i = 0; i < squeeze_workers_per_database; i++)
			{
				worker = &SqueezeWorkers[i];
				if (worker->handle == NULL)
				{
					Assert(worker->task == NULL);

					found = true;
					break;
				}
			}
			if (!found)
				/* Should not happen, given we check of navail above ... */
				ereport(ERROR,
						(errmsg("cannot launch more squeeze workers")));

			task_id = lfirst_int(lc);
			worker->task = &workerData->tasks[task_id];

			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();

			/*
			 * The handle (and possibly other allocations) must survive
			 * the current transaction.
			 */
			old_cxt = MemoryContextSwitchTo(sched_cxt);
			too_many = !start_worker_internal(false, task_id,
											  &worker->handle);
			MemoryContextSwitchTo(old_cxt);

			/*
			 * workers_per_database can be exceeded if we are using all the
			 * workers allowed by this limit and the squeeze_table() function
			 * manages to use some of them at the same time. If we release the
			 * task, the query should fetch it again.
			 */
			if (too_many)
			{
				release_task(worker->task, false);
				worker->task = NULL;
				worker->handle = NULL;
			}
			CommitTransactionCommand();
		}

		/* Check later if any table meets the schedule. */
		delay = worker_naptime * 1000L;
	}

	MemoryContextDelete(sched_cxt);
}

/* TODO No longer a loop - rename? Or get rid of the function altogether? */
static void
squeeze_worker_loop(int task_id)
{
	MemoryContext task_cxt;

	/*
	 * Memory context for auxiliary per-task allocations.
	 */
	task_cxt = AllocSetContextCreate(TopMemoryContext,
									 "pg_squeeze task context",
									 ALLOCSET_DEFAULT_SIZES);

	/* Process the assigned task. */
	process_task(task_id, task_cxt);

	/*
	 * Regarding the replication slot, ReplicationSlotInitialize() callback
	 * should be setup via before_shmem_exit() during bgworker startup.
	 */

	MemoryContextDelete(task_cxt);
}

/*
 * process_next_task() function used to be implemented in pl/pgsql. However,
 * since it calls the squeeze_table() function and since the commit 240e0dbacd
 * in PG core makes it impossible to call squeeze_table() via the postgres
 * executor, this function must be implemented in C and call squeeze_table()
 * directly.
 *
 * task_id is an index into the shared memory array of tasks
 */
static void
process_task(int task_id, MemoryContext task_cxt)
{
	int			i;
	Name		relschema,
		relname;
	Name		cl_index = NULL;
	Name		rel_tbsp = NULL;
	ArrayType  *ind_tbsps = NULL;
	MemoryContext oldcxt;
	WorkerTask *task;
	uint32		arr_size;
	TimestampTz start_ts;
	bool		success;
	RangeVar   *relrv;
	Relation	rel;
	Oid			relid;
	bool		found;
	ErrorData  *edata;

	Assert(MyWorkerTask == NULL);

	/* First, check for a task in the shared memory. */
	Assert(task_id < NUM_WORKER_TASKS);
	task = MyWorkerTask = &workerData->tasks[task_id];

	/*
	 * Once the backend sets "assigned" and the worker is launched, only the
	 * worker is expected to change the task, so access it w/o locking.
	 */
	Assert(task->assigned && task->slot == NULL);
	task->slot = MyWorkerSlot;

	relschema = &task->relschema;
	relname = &task->relname;
	if (strlen(NameStr(task->indname)) > 0)
		cl_index = &task->indname;
	if (strlen(NameStr(task->tbspname)) > 0)
		rel_tbsp = &task->tbspname;

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

	/* Now process the task. */
	ereport(DEBUG1,
			(errmsg("task for table %s.%s is ready for processing",
					NameStr(*relschema), NameStr(*relname))));

	cl_index = NULL;
	rel_tbsp = NULL;

	/* Retrieve relid of the table. */
	StartTransactionCommand();
	relrv = makeRangeVar(NameStr(*relschema), NameStr(*relname), -1);
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
		squeeze_handle_error_app(edata, task);
		success = false;
	}
	PG_END_TRY();

	if (!success)
	{
		release_task(MyWorkerTask, true);
		return;
	}

	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
	found = false;
	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];

		if (slot->dbid == MyDatabaseId &&
			slot->relid == relid)
		{
			found = true;
			break;
		}
	}
	if (found)
	{
		/* XXX The scheduler should not allow this.? */
		ereport(DEBUG1,
				(errmsg("task for table %s.%s is being processed by another worker",
						NameStr(*relschema), NameStr(*relname))));
		LWLockRelease(workerData->lock);
		release_task(MyWorkerTask, true);
		return;
	}
	/* Declare that this worker takes care of the relation. */
	Assert(MyWorkerSlot->dbid == MyDatabaseId);
	MyWorkerSlot->relid = relid;
	reset_progress(&MyWorkerSlot->progress);
	LWLockRelease(workerData->lock);
	/*
	 * The table can be dropped now, created again (with a different OID),
	 * scheduled for processing and picked by another worker. The worst case
	 * is that the table will be squeezed twice, so the time spent by the
	 * worker that finished first will be wasted. However such a situation is
	 * not really likely to happen.
	 */

	/*
	 * The session origin will be used to mark WAL records produced by the
	 * pg_squeeze extension itself so that they can be skipped easily during
	 * decoded. (We avoid the decoding for performance reasons. Even if those
	 * changes were decoded, our output plugin should not apply them because
	 * squeeze_table_impl() exits before its transaction commits.)
	 *
	 * The origin needs to be created in a separate transaction because other
	 * workers, waiting for an unique origin id, need to wait for this
	 * transaction to complete. If we called both replorigin_create() and
	 * squeeze_table_impl() in the same transaction, the calls of
	 * squeeze_table_impl() would effectively get serialized.
	 *
	 * Errors are not catched here. If an operation as trivial as this fails,
	 * worker's exit is just the appropriate action.
	 */
	manage_session_origin(relid);

	/* Perform the actual work. */
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	start_ts = GetCurrentStatementStartTimestamp();
	success = squeeze_table_impl(relschema, relname, cl_index,
								 rel_tbsp, ind_tbsps, &edata, task_cxt);

	if (success)
	{
		CommitTransactionCommand();

		/*
		 * Now that the transaction is committed, we can run a new one to
		 * drop the origin.
		 */
		Assert(replorigin_session_origin != InvalidRepOriginId);

		manage_session_origin(InvalidOid);
	}
	else
	{
		/*
		 * The transaction should be aborted by squeeze_table_impl().
		 */
		squeeze_handle_error_app(edata, task);
	}

	/* Insert an entry into the "squeeze.log" table. */
	if (success)
	{
		Oid			outfunc;
		bool		isvarlena;
		FmgrInfo	fmgrinfo;
		char	   *start_ts_str;
		StringInfoData	query;

		initStringInfo(&query);
		StartTransactionCommand();
		getTypeOutputInfo(TIMESTAMPTZOID, &outfunc, &isvarlena);
		fmgr_info(outfunc, &fmgrinfo);
		start_ts_str = OutputFunctionCall(&fmgrinfo, TimestampTzGetDatum(start_ts));
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
						 NameStr(*relschema),
						 NameStr(*relname),
						 start_ts_str,
						 MyWorkerProgress->ins_initial,
						 MyWorkerProgress->ins,
						 MyWorkerProgress->upd,
						 MyWorkerProgress->del);
		run_command(query.data, SPI_OK_INSERT);

		if (task->task_id >= 0)
		{
			/* Finalize the task if it was a scheduled one. */
			resetStringInfo(&query);
			appendStringInfo(&query, "SELECT squeeze.finalize_task(%d)",
							 task->task_id);
			run_command(query.data, SPI_OK_SELECT);

			if (!task->skip_analyze)
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
								 NameStr(*relschema),
								 NameStr(*relname));
				run_command(query.data, SPI_OK_UTILITY);
			}
		}
	}

	release_task(MyWorkerTask, true);

	/* Clear the relid field of this worker's slot. */
	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
	MyWorkerSlot->relid = InvalidOid;
	reset_progress(&MyWorkerSlot->progress);
	LWLockRelease(workerData->lock);
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
squeeze_handle_error_app(ErrorData *edata, WorkerTask *task)
{
	StringInfoData query;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO squeeze.errors(tabschema, tabname, sql_state, err_msg, err_detail) \
VALUES ('%s', '%s', '%s', '%s', '%s')",
					 NameStr(task->relschema),
					 NameStr(task->relname),
					 unpack_sql_state(edata->sqlerrcode),
					 edata->message,
					 edata->detail ? edata->detail : "");
	run_command(query.data, SPI_OK_INSERT);

	if (task->task_id >= 0)
	{
		/* If the active task failed too many times, cancel it. */
		resetStringInfo(&query);
		if (task->last_try)
		{
			appendStringInfo(&query,
							 "SELECT squeeze.cancel_task(%d)",
							 task->task_id);
			run_command(query.data, SPI_OK_SELECT);
		}
		else
		{
			/* Account for the current attempt. */
			appendStringInfo(&query,
							 "UPDATE squeeze.tasks SET tried = tried + 1 WHERE id = %d",
							 task->task_id);
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
interrupt_worker(WorkerTask *task)
{
	SpinLockAcquire(&task->mutex);
	task->exit_requested = true;
	SpinLockRelease(&task->mutex);
}

static void
release_task(WorkerTask *task, bool worker)
{
	SpinLockAcquire(&task->mutex);
	if (worker)
	{
		/* Called from background worker. */
		task->slot = NULL;
		Assert(task == MyWorkerTask);
		task->exit_requested = false;
		MyWorkerTask = NULL;
	}
	else
	{
		/* Called from backend or from the scheduler worker. */

		Assert(task->assigned);
		task->assigned = false;
	}
	SpinLockRelease(&task->mutex);
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
 *
 * 'rc' is the expected return code.
 *
 * The return value tells how many tuples are returned by the query.
 */
static uint64
run_command(char *command, int rc)
{
	int			ret;
	uint64		ntup = 0;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, command);

	ret = SPI_execute(command, false, 0);
	if (ret != rc)
		elog(ERROR, "command failed: %s", command);

	if (rc == SPI_OK_SELECT || rc == SPI_OK_INSERT_RETURNING ||
		rc == SPI_OK_DELETE_RETURNING || rc == SPI_OK_UPDATE_RETURNING)
	{
#if PG_VERSION_NUM >= 130000
		ntup = SPI_tuptable->numvals;
#else
		ntup = SPI_processed;
#endif
	}
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);

	return ntup;
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
