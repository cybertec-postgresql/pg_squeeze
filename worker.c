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
#include "replication/snapbuild.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#if PG_VERSION_NUM >= 160000
#include "utils/backend_status.h"
#endif
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "pg_squeeze.h"

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

/*
 * Indicates that the squeeze worker was launched by an user backend (using
 * the squeeze_table() function), as opposed to the scheduler worker.
 */
static bool	am_i_standalone = false;

/*
 * The shmem_request_hook_type hook was introduced in PG 15. Since the number
 * of slots depends on the max_worker_processes GUC, the maximum number of
 * squeeze workers must be a compile time constant for PG < 15.
 *
 * XXX Regarding PG < 15: maybe we don't need to worry about dependency on an
 * in-core GUC - the value should be known at load time and no other loadable
 * module should be able to change it before we start the shared memory
 * allocation.
 */
static int
max_squeeze_workers(void)
{
#if PG_VERSION_NUM >= 150000
	return max_worker_processes;
#else
#define	MAX_SQUEEZE_WORKERS	32

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
	 * Has cleanup after restart completed? The first worker launched after
	 * server restart should set this flag.
	 */
	bool		cleanup_done;

	/*
	 * A lock to synchronize access to slots. Lock in exclusive mode to add /
	 * remove workers, in shared mode to find information on them.
	 *
	 * It's also used to synchronize task creation, so that we don't have more
	 * than one task per table.
	 */
	LWLock	   *lock;

	int			nslots;			/* size of the array */
	WorkerSlot	slots[FLEXIBLE_ARRAY_MEMBER];
} WorkerData;

static WorkerData *workerData = NULL;

/* Local pointer to the slot in the shared memory. */
WorkerSlot *MyWorkerSlot = NULL;

/* Local pointer to the task in the shared memory. */
WorkerTask *MyWorkerTask = NULL;

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

static SqueezeWorker	*squeezeWorkers = NULL;
static int	squeezeWorkerCount = 0;
/*
 * One slot per worker, but the count is stored separately because cleanup is
 * also done separately.
 */
static ReplSlotStatus	*squeezeWorkerSlots = NULL;
static int	squeezeWorkerSlotCount = 0;

#define	REPL_SLOT_PREFIX	"pg_squeeze_slot_"
#define	REPL_PLUGIN_NAME	"pg_squeeze"

static void interrupt_worker(WorkerTask *task);
static void release_task(WorkerTask *task, bool worker);
static void squeeze_handle_error_app(ErrorData *edata, WorkerTask *task);

static WorkerTask *get_unused_task(Oid dbid, Name relschema, Name relname,
								   int *task_idx, bool *duplicate);
static void initialize_worker_task(WorkerTask *task, int task_id, Name indname,
								   Name tbspname, ArrayType *ind_tbsps,
								   bool last_try, bool skip_analyze,
								   int max_xlock_time);
static bool start_worker_internal(bool scheduler, int task_idx,
								  BackgroundWorkerHandle **handle);

static void worker_sighup(SIGNAL_ARGS);
static void worker_sigterm(SIGNAL_ARGS);

static void scheduler_worker_loop(void);
static void cleanup_workers_and_tasks(bool interrupt);
static void wait_for_worker_shutdown(SqueezeWorker *worker);
static void process_task(int task_idx);
static void create_replication_slots(int nslots, MemoryContext mcxt);
static void drop_replication_slots(void);
static void cleanup_after_server_start(int task_idx);
static void cleanup_repl_origins(void);
static void cleanup_repl_slots(void);
static Snapshot build_historic_snapshot(SnapBuild *builder);
static void process_task_internal(MemoryContext task_cxt);

static uint64 run_command(char *command, int rc);

static Size
worker_shmem_size(void)
{
	Size		size;

	size = offsetof(WorkerData, slots);
	size = add_size(size, mul_size(max_squeeze_workers(),
								   sizeof(WorkerSlot)));
	return size;
}


#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;

void
squeeze_save_prev_shmem_request_hook(void)
{
	prev_shmem_request_hook = shmem_request_hook;
}
#endif

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

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void
squeeze_save_prev_shmem_startup_hook(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
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
			task->in_use = false;
			task->exit_requested = false;
			task->worker_state = WTS_UNUSED;
			task->error_msg[0] = '\0';
			task->dbid = InvalidOid;
			NameStr(task->relschema)[0] = '\0';
			NameStr(task->relname)[0] = '\0';
			SpinLockInit(&task->mutex);
		}

		workerData->lock = &locks->lock;
		workerData->cleanup_done = false;
		workerData->nslots = max_squeeze_workers();

		for (i = 0; i < workerData->nslots; i++)
		{
			WorkerSlot *slot = &workerData->slots[i];

			slot->dbid = InvalidOid;
			slot->relid = InvalidOid;
			SpinLockInit(&slot->mutex);
			MemSet(&slot->progress, 0, sizeof(WorkerProgress));
			slot->pid = InvalidPid;
		}
	}

	LWLockRelease(AddinShmemInitLock);
}

static	List	*task_idxs = NIL;

/* Mark this worker's slot unused. */
static void
worker_shmem_shutdown(int code, Datum arg)
{
	/* exiting before the slot was initialized? */
	if (MyWorkerSlot)
	{
		/*
		 * Use spinlock to make sure that invalid dbid implies that the
		 * clearing is done.
		 */
		SpinLockAcquire(&MyWorkerSlot->mutex);
		Assert(MyWorkerSlot->dbid != InvalidOid);
		MyWorkerSlot->dbid = InvalidOid;
		MyWorkerSlot->relid = InvalidOid;
		MyWorkerSlot->pid = InvalidPid;
		MemSet(&MyWorkerSlot->progress, 0, sizeof(WorkerProgress));
		SpinLockRelease(&MyWorkerSlot->mutex);

		/* This shouldn't be necessary, but ... */
		MyWorkerSlot = NULL;
	}

	if (MyWorkerTask)
		release_task(MyWorkerTask, true);

	if (am_i_scheduler)
		/*
		 * Cleanup. Here, instead of just waiting for workers to finish, we
		 * ask them to exit as soon as possible.
		 */
		cleanup_workers_and_tasks(true);
	else if (am_i_standalone)
		/*
		 * Note that the worker launched by the squeeze_table() function needs
		 * to do the cleanup on its own.
		 */
		drop_replication_slots();

	/*
	 * Release LW locks acquired outside transaction.
	 *
	 * There's at least one such case: when the worker is looking for a slot
	 * in the shared memory - see squeeze_worker_main().
	 */
	LWLockReleaseAll();
}

/*
 * Start the scheduler worker.
 */
PG_FUNCTION_INFO_V1(squeeze_start_worker);
Datum
squeeze_start_worker(PG_FUNCTION_ARGS)
{
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("pg_squeeze cannot be used during recovery.")));

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to start squeeze worker"))));

	start_worker_internal(true, -1, NULL);

	PG_RETURN_VOID();
}

/*
 * Stop the scheduler worker.
 */
PG_FUNCTION_INFO_V1(squeeze_stop_worker);
Datum
squeeze_stop_worker(PG_FUNCTION_ARGS)
{
	int			i;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to stop squeeze worker"))));

	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];
		Oid		dbid;
		bool	scheduler;

		SpinLockAcquire(&slot->mutex);
		dbid = slot->dbid;
		scheduler = slot->scheduler;
		SpinLockRelease(&slot->mutex);

		if (dbid == MyDatabaseId && scheduler)
		{
			kill(slot->pid, SIGTERM);

			/*
			 * There should only be one scheduler per database. (It'll stop
			 * the squeeze workers it launched.)
			 */
			break;
		}
	}

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
	int		task_idx;
	WorkerTask *task = NULL;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	char	*error_msg = NULL;
	bool	task_exists;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("pg_squeeze cannot be used during recovery.")));

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
	task = get_unused_task(MyDatabaseId, relschema, relname, &task_idx,
						   &task_exists);
	if (task == NULL)
	{
		if (task_exists)
			ereport(ERROR,
					(errmsg("task for relation \"%s\".\"%s\" already exists",
							NameStr(*relschema), NameStr(*relname))));
		else
			ereport(ERROR, (errmsg("too many concurrent tasks in progress")));
	}

	/* Fill-in the remaining task information. */
	initialize_worker_task(task, -1, indname, tbspname, ind_tbsps, false,
						   true, squeeze_max_xlock_time);
	/*
	 * Unlike scheduler_worker_loop() we cannot build the snapshot here, the
	 * worker will do. (It will also create the replication slot.) This is
	 * related to the variable am_i_standalone in process_task().
	 */

	/* Start the worker to handle the task. */
	if (!start_worker_internal(false, task_idx, &handle))
	{
		/*
		 * The worker could not even get registered, so it won't set its
		 * status to WTS_UNUSED. Make sure the task does not leak.
		 */
		release_task(task, true);

		/* Do what the task requester is responsible for. */
		release_task(task, false);

		ereport(ERROR,
				(errmsg("squeeze worker could not start")),
				(errhint("consider increasing \"max_worker_processes\" or decreasing \"squeeze.workers_per_database\"")));
	}

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

	if (strlen(task->error_msg) > 0)
		error_msg = pstrdup(task->error_msg);

	release_task(task, false);

	if (error_msg)
		ereport(ERROR, (errmsg("%s", error_msg)));

	PG_RETURN_VOID();
}

/*
 * Returns a newly assigned task. Return NULL if there's no unused slot or a
 * task already exists for given relation.
 *
 * The index in the task array is returned in *task_idx.
 *
 * The returned task ahs 'dbid', 'relschema' and 'relname' fields initialized.
 *
 * If NULL is returned, *duplicate tells whether it's due to an existing task
 * for given relation.
 */
static WorkerTask *
get_unused_task(Oid dbid, Name relschema, Name relname, int *task_idx,
				bool *duplicate)
{
	int		i;
	WorkerTask	*task;
	WorkerTask	*result = NULL;
	int			res_idx = -1;

	*duplicate = false;

	/*
	 * Find an unused task and make sure that a valid task does not exist for
	 * the same relation.
	 */
	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);
	for (i = 0; i < NUM_WORKER_TASKS; i++)
	{
		bool	needs_check = false;

		task = &workerData->tasks[i];
		SpinLockAcquire(&task->mutex);
		if (task->in_use || task->worker_state != WTS_UNUSED)
		{
			/*
			 * Consider tasks which might be in progress possible duplicates
			 * of the task we're going to submit.
			 */
			needs_check = true;
		}
		else if (result == NULL)
		{
			/* Result candidate */
			result = task;
			res_idx = i;
		}
		/*
		 * String comparisons shouldn't take place under spinlock. (Those
		 * fields aren't protected by the spinlock anyway.)
		 */
		SpinLockRelease(&task->mutex);

		if (needs_check)
		{
			/*
			 * The strings are only set while workerData->lock is held in
			 * exclusive mode (see below), so we can safely check them here.
			 */
			if (task->dbid == dbid &&
				strcmp(NameStr(task->relschema), NameStr(*relschema)) == 0 &&
				strcmp(NameStr(task->relname), NameStr(*relname)) == 0)
			{
				result = NULL;
				res_idx = -1;
				*duplicate = true;

				goto done;
			}
		}
	}
	if (result == NULL)
		goto done;

	/*
	 * No one else should be interested in this task, so don't use spinlock
	 * below.
	 *
	 * Make sure that no other backend can use the task.
	 */
	result->in_use = true;
	/* Set the initial state for the worker. */
	result->worker_state = WTS_INIT;

	/* Initialize the fields we use to check uniqueness of the task. */
	result->dbid = dbid;
	namestrcpy(&result->relschema, NameStr(*relschema));
	namestrcpy(&result->relname, NameStr(*relname));

done:
	LWLockRelease(workerData->lock);
	*task_idx = res_idx;
	return result;
}

/*
 * Fill-in "user data" of WorkerTask. task_id, dbid, relschema and relname
 * should already be set.
 */
static void
initialize_worker_task(WorkerTask *task, int task_id, Name indname,
					   Name tbspname, ArrayType *ind_tbsps, bool last_try,
					   bool skip_analyze, int max_xlock_time)
{
	StringInfoData	buf;

	initStringInfo(&buf);

	task->task_id = task_id;
	appendStringInfo(&buf,
					 "squeeze worker task: id=%d, relschema=%s, relname=%s",
					 task->task_id, NameStr(task->relschema),
					 NameStr(task->relname));

	if (indname)
	{
		namestrcpy(&task->indname, NameStr(*indname));
		appendStringInfo(&buf, ", indname: %s", NameStr(task->indname));
	}
	else
		NameStr(task->indname)[0] = '\0';
	if (tbspname)
	{
		namestrcpy(&task->tbspname, NameStr(*tbspname));
		appendStringInfo(&buf, ", tbspname: %s", NameStr(task->tbspname));
	}
	else
		NameStr(task->tbspname)[0] = '\0';
	/* ind_tbsps is in a binary format, don't bother logging it right now. */
	if (ind_tbsps)
	{
		if (VARSIZE(ind_tbsps) > IND_TABLESPACES_ARRAY_SIZE)
			ereport(ERROR, (errmsg("the array of index tablespaces is too big")));
		memcpy(task->ind_tbsps, ind_tbsps, VARSIZE(ind_tbsps));
	}
	else
		SET_VARSIZE(task->ind_tbsps, 0);
	ereport(DEBUG1, (errmsg("%s", buf.data)));
	pfree(buf.data);

	task->error_msg[0] = '\0';
	task->last_try = last_try;
	task->skip_analyze = skip_analyze;
	task->max_xlock_time = max_xlock_time;
}

/*
 * Register either scheduler or squeeze worker, according to the argument.
 *
 * The number of scheduler workers per database is limited by the
 * squeeze_workers_per_database configuration variable.
 *
 * The return value tells whether we could least register the worker.
 */
static bool
start_worker_internal(bool scheduler, int task_idx,
					  BackgroundWorkerHandle **handle)
{
	WorkerConInteractive con;
	BackgroundWorker worker;
	char	   *kind;

	Assert(!scheduler || task_idx < 0);

	/*
	 * Make sure all the task fields are visible to the worker before starting
	 * it. This is similar to the use of the write barrier in
	 * RegisterDynamicBackgroundWorker() in PG core. However, the new process
	 * does not need to use "read barrier" because once it's started, the
	 * shared memory writes done by start_worker_internal() must essentially
	 * have been read. (Otherwise the worker would not start.)
	 */
	if (task_idx >= 0)
		pg_write_barrier();

	kind = scheduler ? "scheduler" : "squeeze";

	con.dbid = MyDatabaseId;
	con.roleid = GetUserId();
	con.scheduler = scheduler;
	con.task_idx = task_idx;
	squeeze_initialize_bgworker(&worker, NULL, &con, MyProcPid);

	ereport(DEBUG1, (errmsg("registering pg_squeeze %s worker", kind)));
	if (!RegisterDynamicBackgroundWorker(&worker, handle))
		return false;

	if (handle == NULL)
		/*
		 * Caller is not interested in the status, the return value does not
		 * matter.
		 */
		return false;

	if (*handle == NULL)
		return false;

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
allocate_worker_con_info(char *dbname, char *rolename)
{
	WorkerConInit *result;

	result = (WorkerConInit *) MemoryContextAllocZero(TopMemoryContext,
													  sizeof(WorkerConInit));
	result->dbname = MemoryContextStrdup(TopMemoryContext, dbname);
	result->rolename = MemoryContextStrdup(TopMemoryContext, rolename);
	return result;
}

/*
 * Initialize the worker and pass connection info in the appropriate form.
 *
 * 'con_init' is passed only for the scheduler worker, whereas
 * 'con_interactive' can be passed for both squeeze worker and scheduler
 * worker.
 */
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
		scheduler = true;
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

void
squeeze_worker_main(Datum main_arg)
{
	Datum		arg;
	int			i;
	bool		found_scheduler;
	int			nworkers;
	int			task_idx = -1;

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
		am_i_scheduler = true;
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

		task_idx = con.task_idx;
	}

	found_scheduler = false;
	nworkers = 0;
	/*
	 * Find and initialize a slot for this worker.
	 *
	 * While doing that, make sure that there is no more than one scheduler
	 * and no more than squeeze_workers_per_database workers running on this
	 * database.
	 *
	 * Exclusive lock is needed to make sure that the maximum number of
	 * workers is not exceeded due to race conditions.
	 */
	Assert(MyWorkerSlot == NULL);
	LWLockAcquire(workerData->lock, LW_EXCLUSIVE);

	/*
	 * The first worker after restart is responsible for cleaning up
	 * replication slots and/or origins that other workers could not remove
	 * due to server crash. Do that while holding the exclusive lock, as
	 * concurrency makes no sense here. That also ensures that the other
	 * workers wait for the cleanup to finish instead of complaining about the
	 * existing slots / origins.
	 */
	if (!am_i_scheduler && !workerData->cleanup_done)
	{
		cleanup_after_server_start(task_idx);
		workerData->cleanup_done = true;
	}

	for (i = 0; i < workerData->nslots; i++)
	{
		WorkerSlot *slot = &workerData->slots[i];
		Oid		dbid;

		/*
		 * The spinlock might seem unnecessary, but w/o that it could happen
		 * that we saw 'dbid' invalid (i.e. ready to use) while another worker
		 * is still clearing the other fields (before exit) and thus it can
		 * overwrite our settings - see worker_shmem_shutdown().
		 */
		SpinLockAcquire(&slot->mutex);
		dbid = slot->dbid;
		SpinLockRelease(&slot->mutex);

		if (dbid == MyDatabaseId)
		{
			if (am_i_scheduler && slot->scheduler)
			{
				elog(WARNING,
					 "one scheduler worker already running on database oid=%u",
					 MyDatabaseId);

				found_scheduler = true;
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
		else if (dbid == InvalidOid && MyWorkerSlot == NULL)
			MyWorkerSlot = slot;
	}

	if (found_scheduler || (nworkers >= squeeze_workers_per_database))
	{
		LWLockRelease(workerData->lock);
		goto done;
	}

	/*
	 * Fill-in all the information we have. (relid will be set in
	 * process_task() unless this worker is a scheduler.)
	 */
	if (MyWorkerSlot)
	{
		WorkerSlot *slot = MyWorkerSlot;

		/*
		 * The spinlock is probably not necessary here (no one else should be
		 * interested in this slot).
		 */
		SpinLockAcquire(&slot->mutex);
		slot->dbid = MyDatabaseId;
		Assert(slot->relid == InvalidOid);
		Assert(slot->pid == InvalidPid);
		slot->pid = MyProcPid;
		slot->scheduler = am_i_scheduler;
		MemSet(&slot->progress, 0, sizeof(WorkerProgress));
		SpinLockRelease(&slot->mutex);
	}
	LWLockRelease(workerData->lock);

	/* Is there no unused slot? */
	if (MyWorkerSlot == NULL)
	{
		elog(WARNING,
			 "no unused slot found for pg_squeeze worker process");

		goto done;
	}

	if (am_i_scheduler)
		scheduler_worker_loop();
	else
		process_task(task_idx);

done:
	if (task_idx >= 0)
	{
		/* Make sure that worker_shmem_shutdown() releases the task. */
		Assert(task_idx < NUM_WORKER_TASKS);
		MyWorkerTask = &workerData->tasks[task_idx];
	}

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
	MemoryContext	sched_cxt, old_cxt;

	/* Context for allocations which cannot be freed too early. */
	sched_cxt = AllocSetContextCreate(TopMemoryContext,
									  "pg_squeeze scheduler context",
									  ALLOCSET_DEFAULT_SIZES);
	while (!got_sigterm)
	{
		StringInfoData	query;
		int			rc;
		uint64		ntask;
		TupleDesc	tupdesc;
		TupleTableSlot *slot;
		ListCell	*lc;
		int		nslots;

		/*
		 * Make sure all the workers we launched in the previous loop and
		 * their tasks and replication slots are cleaned up.
		 */
		cleanup_workers_and_tasks(false);
		/* Free the corresponding memory. */
		MemoryContextReset(sched_cxt);

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
			"LIMIT %d", squeeze_workers_per_database);

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

		ereport(DEBUG1, (errmsg("scheduler worker: %zu tasks available",
								ntask)));

		if (ntask > 0)
		{
			tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
			slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
		}

		/* Initialize the task slots. */
		for (i = 0; i < ntask; i++)
		{
			int		idx, task_id;
			WorkerTask	*task;
			HeapTuple	tup;
			Datum		datum;
			bool		isnull;
			Name	relschema, relname, cl_index, rel_tbsp;
			ArrayType *ind_tbsps;
			bool		last_try;
			bool		skip_analyze;
			bool		task_exists = false;

			cl_index = NULL;
			rel_tbsp = NULL;
			ind_tbsps = NULL;

			/* Retrieve the tuple attributes and use them to fill the task. */
			tup = heap_copytuple(SPI_tuptable->vals[i]);
			ExecClearTuple(slot);
			ExecStoreHeapTuple(tup, slot, true);

			datum = slot_getattr(slot, 1, &isnull);
			Assert(!isnull);
			task_id = DatumGetInt32(datum);

			datum = slot_getattr(slot, 2, &isnull);
			Assert(!isnull);
			relschema = DatumGetName(datum);

			datum = slot_getattr(slot, 3, &isnull);
			Assert(!isnull);
			relname = DatumGetName(datum);

			task = get_unused_task(MyDatabaseId, relschema, relname, &idx,
								   &task_exists);
			if (task == NULL)
			{
				if (task_exists)
				{
					/* Already in progress, go for the next one. */
					ereport(WARNING,
							(errmsg("task already exists for table \"%s\".\"%s\"",
									NameStr(*relschema), NameStr(*relname))));
					continue;
				}
				else
				{
					/*
					 * No point in fetching the remaining columns if all the
					 * tasks are already used.
					 */
					ereport(WARNING,
							(errmsg("the task queue is currently full")));
					break;
				}
			}

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
			initialize_worker_task(task, task_id, cl_index, rel_tbsp,
								   ind_tbsps, last_try, skip_analyze,
								   /* XXX Should max_xlock_time be added to
									* squeeze.tables ? */
								   0);

			/* The list must survive SPI_finish(). */
			old_cxt = MemoryContextSwitchTo(sched_cxt);
			task_idxs = lappend_int(task_idxs, idx);
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
		pgstat_report_stat(false);

		/* Initialize the array to track the workers we start. */
		squeezeWorkerCount = nslots = list_length(task_idxs);

		if (squeezeWorkerCount > 0)
		{
			/*
			 * The worker info should be in the sched_cxt which we reset at
			 * the top of each iteration.
			 */
			squeezeWorkers = (SqueezeWorker *) MemoryContextAllocZero(sched_cxt,
																	  squeezeWorkerCount *
																	  sizeof(SqueezeWorker));

			/* Create and initialize the replication slot for each worker. */
			PG_TRY();
			{
				create_replication_slots(nslots, sched_cxt);
			}
			PG_CATCH();
			{
				foreach(lc, task_idxs)
				{
					int	task_idx = lfirst_int(lc);
					WorkerTask	*task = &workerData->tasks[task_idx];

					/*
					 * worker_shmem_shutdown() will call
					 * release_task(worker=false) but we need to do it here on
					 * behalf of the workers which will never start.
					 */
					release_task(task, true);
				}

				PG_RE_THROW();
			}
			PG_END_TRY();

			/*
			 * Now that the transaction has committed, we can start the
			 * workers. (start_worker_internal() needs to run in a transaction
			 * because it does access the system catalog.)
			 */
			i = 0;
			foreach(lc, task_idxs)
			{
				SqueezeWorker	*worker;
				int	task_idx;
				bool	registered;

				worker = &squeezeWorkers[i];
				worker->handle = NULL;
				task_idx = lfirst_int(lc);
				worker->task = &workerData->tasks[task_idx];
				worker->task->repl_slot = squeezeWorkerSlots[i];

				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();

				/*
				 * The handle (and possibly other allocations) must survive
				 * the current transaction.
				 */
				old_cxt = MemoryContextSwitchTo(sched_cxt);
				registered = start_worker_internal(false, task_idx, &worker->handle);
				MemoryContextSwitchTo(old_cxt);

				if (!registered)
				{
					/*
					 * The worker could not even get registered, so it won't
					 * set its status to WTS_UNUSED. Make sure the task does
					 * not leak.
					 */
					release_task(worker->task, true);

					ereport(ERROR,
							(errmsg("squeeze worker could not start")),
							(errhint("consider increasing \"max_worker_processes\" or decreasing \"squeeze.workers_per_database\"")));

				}


				CommitTransactionCommand();

				i++;
			}
		}

		/* Check later if any table meets the schedule. */
		delay = worker_naptime * 1000L;
	}

	/*
	 * Do not reset/delete sched_cxt, worker_shmem_shutdown() may need the
	 * information it contains.
	 */
}

static void
cleanup_workers_and_tasks(bool interrupt)
{
	SqueezeWorker	*worker;
	int	i;
	ListCell	*lc;

	if (interrupt)
	{
		/* Notify the tasks that they should exit. */
		for (i = 0; i < squeezeWorkerCount; i++)
		{
			worker = &squeezeWorkers[i];
			if (worker->task)
				interrupt_worker(worker->task);
		}
	}

	/*
	 * Wait until all the workers started in the previous loops have
	 * finished.
	 */
	for (i = 0; i < squeezeWorkerCount; i++)
	{
		worker = &squeezeWorkers[i];

		/* Not even started or already stopped? */
		if (worker->handle == NULL)
			continue;

		wait_for_worker_shutdown(worker);
	}
	squeezeWorkerCount = 0;
	/* The reset of sched_cxt will free the memory.  */
	squeezeWorkers = NULL;

	/* Release the corresponding tasks. */
	foreach(lc, task_idxs)
	{
		int	task_idx = lfirst_int(lc);
		WorkerTask	*task = &workerData->tasks[task_idx];

		release_task(task, false);
	}
	task_idxs = NIL;

	/* Drop the replication slots. */
	if (squeezeWorkerSlotCount > 0)
		drop_replication_slots();
}

static void
wait_for_worker_shutdown(SqueezeWorker *worker)
{
	BgwHandleStatus	status;

	status = WaitForBackgroundWorkerShutdown(worker->handle);
	if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(ERROR,
				(errmsg("the postmaster died before the squeeze worker could finish"),
				 errhint("More details may be available in the server log.")));
	}
	/*
	 * WaitForBackgroundWorkerShutdown() should not return anything
	 * else.
	 */
	Assert(status == BGWH_STOPPED);

	pfree(worker->handle);
	worker->handle = NULL;
}

static void
process_task(int task_idx)
{
	MemoryContext task_cxt;
	ErrorData  *edata;

	/*
	 * Memory context for auxiliary per-task allocations.
	 */
	task_cxt = AllocSetContextCreate(TopMemoryContext,
									 "pg_squeeze task context",
									 ALLOCSET_DEFAULT_SIZES);

	Assert(task_idx < NUM_WORKER_TASKS);
	MyWorkerTask = &workerData->tasks[task_idx];

	squeeze_max_xlock_time = MyWorkerTask->max_xlock_time;

	/* Process the assigned task. */
	PG_TRY();
	{
		process_task_internal(task_cxt);
	}
	PG_CATCH();
	{
		squeeze_handle_error_db(&edata, task_cxt);
		squeeze_handle_error_app(edata, MyWorkerTask);

		/*
		 * Not sure it makes sense to rethrow the ERROR. The worker is going
		 * to exit anyway.
		 */
	}
	PG_END_TRY();

	MemoryContextDelete(task_cxt);
}

/*
 * Create a replication slot for each squeeze worker and find the start point
 * for logical decoding.
 *
 * We create and initialize all the slots at once because
 * DecodingContextFindStartpoint() waits for the running transactions to
 * complete. If each worker had to initialize its slot, it'd have wait until
 * the other worker(s) are done with their current job (which usually takes
 * some time), so the workers wouldn't actually do their work in parallel.
 */
static void
create_replication_slots(int nslots, MemoryContext mcxt)
{
	uint32		i;
	ReplSlotStatus	*res_ptr;
	MemoryContext	old_cxt;

	Assert(squeezeWorkerSlots == NULL && squeezeWorkerSlotCount == 0);

	/*
	 * Use a transaction so that all the slot related locks are freed on ERROR
	 * and thus drop_replication_slots() can do its work.
	 */
	StartTransactionCommand();

#if PG_VERSION_NUM >= 150000
	CheckSlotPermissions();
#endif
	CheckLogicalDecodingRequirements();

	/*
	 * We are in a transaction, so make sure various allocations survive the
	 * transaction commit.
	 */
	old_cxt = MemoryContextSwitchTo(mcxt);
	squeezeWorkerSlots = (ReplSlotStatus *) palloc0(nslots *
													sizeof(ReplSlotStatus));

	res_ptr = squeezeWorkerSlots;

	/*
	 * XXX It might be faster if we created one slot using the API and the
	 * other ones by copying, however pg_copy_logical_replication_slot()
	 * passes need_full_snapshot=false to CreateInitDecodingContext().
	 */
	for (i = 0; i < nslots; i++)
	{
		char	name[NAMEDATALEN];
		LogicalDecodingContext *ctx;
		ReplicationSlot *slot;
		Snapshot	snapshot;
		Size		snap_size;
		char		*snap_dst;
		int		slot_nr;

		if (am_i_standalone)
		{
			/*
			 * squeeze_table() can be called concurrently (for different
			 * tables), so make sure that each call generates an unique slot
			 * name.
			 */
			Assert(nslots == 1);
			slot_nr = MyProcPid;
		}
		else
			slot_nr = i;

		snprintf(name, NAMEDATALEN, REPL_SLOT_PREFIX "%u_%u", MyDatabaseId,
				 slot_nr);

#if PG_VERSION_NUM >= 140000
		ReplicationSlotCreate(name, true, RS_PERSISTENT, false);
#else
		ReplicationSlotCreate(name, true, RS_PERSISTENT);
#endif
		slot = MyReplicationSlot;

		/*
		 * Save the name early so that the slot gets cleaned up if the steps
		 * below throw ERROR.
		 */
		namestrcpy(&res_ptr->name, slot->data.name.data);
		squeezeWorkerSlotCount++;

		/*
		 * Neither prepare_write nor do_write callback nor update_progress is
		 * useful for us.
		 *
		 * Regarding the value of need_full_snapshot, we pass true to protect
		 * its data from VACUUM. Otherwise the historical snapshot we use for
		 * the initial load could miss some data. (Unlike logical decoding, we
		 * need the historical snapshot for non-catalog tables.)
		 */
		ctx = CreateInitDecodingContext(REPL_PLUGIN_NAME,
										NIL,
										true,
										InvalidXLogRecPtr,
#if PG_VERSION_NUM >= 130000
										XL_ROUTINE(.page_read = read_local_xlog_page,
												   .segment_open = wal_segment_open,
												   .segment_close = wal_segment_close),
#else
										logical_read_local_xlog_page,
#endif
										NULL, NULL, NULL);


		/*
		 * We don't have control on setting fast_forward, so at least check
		 * it.
		 */
		Assert(!ctx->fast_forward);

		SpinLockAcquire(&slot->mutex);
		Assert(TransactionIdIsValid(slot->effective_xmin) &&
			   !TransactionIdIsValid(slot->data.xmin));
		/* Prevent ReplicationSlotRelease() from clearing effective_xmin. */
		slot->data.xmin = slot->effective_xmin;
		SpinLockRelease(&slot->mutex);

		/*
		 * Bring the snapshot builder into the SNAPBUILD_CONSISTENT state so
		 * that the worker can get its snapshot and start decoding
		 * immediately. This is where we might need to wait for other
		 * transactions to finish, so it should not be done by the workers.
		 */
		DecodingContextFindStartpoint(ctx);

		/* Get the values the caller is interested int. */
		res_ptr->confirmed_flush = slot->data.confirmed_flush;

		/*
		 * Unfortunately the API is such that CreateDecodingContext() assumes
		 * need_full_snapshot=false, so the worker won't be able to create the
		 * snapshot for the initial load. Therefore we serialize the snapshot
		 * here and pass the name to the worker via shared memory.
		 */
		snapshot = build_historic_snapshot(ctx->snapshot_builder);
		snap_size = EstimateSnapshotSpace(snapshot);
		if (!am_i_standalone)
		{
			res_ptr->snap_seg = dsm_create(snap_size, 0);
			/*
			 * The current transaction's commit must not detach the
			 * segment.
			 */
			dsm_pin_mapping(res_ptr->snap_seg);
			res_ptr->snap_handle = dsm_segment_handle(res_ptr->snap_seg);
			res_ptr->snap_private = NULL;
			snap_dst = (char *) dsm_segment_address(res_ptr->snap_seg);
		}
		else
		{
			res_ptr->snap_seg = NULL;
			res_ptr->snap_handle = DSM_HANDLE_INVALID;
			snap_dst = res_ptr->snap_private = (char *) palloc(snap_size);
		}
		/*
		 * XXX Should we care about alignment? The function doesn't seem to
		 * need that.
		 */
		SerializeSnapshot(snapshot, snap_dst);

		res_ptr++;

		/*
		 * Done for now, the worker will have to setup the context on its own.
		 */
		FreeDecodingContext(ctx);
		ReplicationSlotRelease();
	}

	MemoryContextSwitchTo(old_cxt);
	CommitTransactionCommand();

	Assert(squeezeWorkerSlotCount == nslots);
}

/*
 * Drop replication slots the worker created. If this is the scheduler worker,
 * we may need to wait for the squeeze workers to release the slots.
 */
static void
drop_replication_slots(void)
{
	int		i;

	/*
	 * Called during normal operation and now called again by the
	 * worker_shmem_shutdown callback?
	 */
	if (squeezeWorkerSlots == NULL)
	{
		Assert(squeezeWorkerSlotCount == 0);
		return;
	}

	/*
	 * ERROR in create_replication_slots() can leave us with one of the slots
	 * acquired, so release it before we start dropping them all.
	 */
	if (MyReplicationSlot)
		ReplicationSlotRelease();

	for (i = 0; i < squeezeWorkerSlotCount; i++)
	{
		ReplSlotStatus	*slot;

		slot = &squeezeWorkerSlots[i];
		if (strlen(NameStr(slot->name)) > 0)
		{
			/* nowait=false, i.e. wait */
			ReplicationSlotDrop(NameStr(slot->name), false);
		}
	}

	squeezeWorkerSlotCount = 0;
	/*
	 * Caller should reset the containing memory context. (Unless this is
	 * called during exit.)
	 */
	squeezeWorkerSlots = NULL;
}

/*
 * The first squeeze worker launched after server start calls this function to
 * make sure that no replication slots / origins exist.
 *
 * task_idx is needed so that error message can be sent to the backend that
 * launched the worker. ERROR is supposed to terminate the worker.
 */
static void
cleanup_after_server_start(int task_idx)
{
	PG_TRY();
	{
		cleanup_repl_origins();
		cleanup_repl_slots();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		Assert(task_idx >= 0);
		MyWorkerTask = &workerData->tasks[task_idx];

		/*
		 * The worker should exit pretty soon, it's o.k. to use
		 * TopMemoryContext (i.e. it causes no real memory leak).
		 */
		squeeze_handle_error_db(&edata, TopMemoryContext);

		PG_RE_THROW();
	}
	PG_END_TRY();

}

/*
 * Sub-routine of cleanup_after_server_start().
 */
static void
cleanup_repl_origins(void)
{
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tuple;
	char	*orig_name;
	List		*origs = NIL;
	ListCell	*lc;

	StartTransactionCommand();

	rel = table_open(ReplicationOriginRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_replication_origin	form;

		form = (Form_pg_replication_origin) GETSTRUCT(tuple);
		orig_name = text_to_cstring(&form->roname);
		origs = lappend(origs, orig_name);
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	foreach(lc, origs)
	{
		orig_name = (char *) lfirst(lc);

		/* Drop the origin iff it looks like one created by pg_squeeze. */
		if (strncmp(orig_name, REPLORIGIN_NAME_PREFIX,
					strlen(REPLORIGIN_NAME_PREFIX)) == 0)
		{
			ereport(DEBUG1,
					(errmsg("cleaning up replication origin \"%s\"",
							orig_name)));
			/* nowait=true because no one should be using the origin. */
			replorigin_drop_by_name(orig_name, false, true);
		}
	}
	list_free(origs);

	CommitTransactionCommand();
}

/*
 * Sub-routine of cleanup_after_server_start().
 */
static void
cleanup_repl_slots(void)
{
	int		slotno;
	List	*slot_names = NIL;

	ereport(ERROR, (errmsg("test")));
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (slotno = 0; slotno < max_replication_slots; slotno++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[slotno];
		ReplicationSlot	slot_contents;
		char	*name;

		if (!slot->in_use)
			continue;

		SpinLockAcquire(&slot->mutex);
		slot_contents = *slot;
		SpinLockRelease(&slot->mutex);

		name = NameStr(slot_contents.data.name);

		if (strncmp(name, REPL_SLOT_PREFIX, strlen(REPL_SLOT_PREFIX)) == 0)
			slot_names = lappend(slot_names, pstrdup(name));
	}
	LWLockRelease(ReplicationSlotControlLock);

	if (list_length(slot_names) > 0)
	{
		ListCell	*lc;

		/*
		 * XXX Is transaction needed here? We do not access system catalogs
		 * and LW locks are released by the worker on exit anyway.
		 */
		foreach(lc, slot_names)
		{
			char	*slot_name = (char *) lfirst(lc);

			ereport(DEBUG1,
					(errmsg("cleaning up replication slot \"%s\"",
							slot_name)));
			ReplicationSlotDrop(slot_name, true);
		}

		list_free_deep(slot_names);
	}
}

/*
 * Wrapper for SnapBuildInitialSnapshot().
 *
 * We do not have to meet the assertions that SnapBuildInitialSnapshot()
 * contains, nor should we set MyPgXact->xmin.
 */
static Snapshot
build_historic_snapshot(SnapBuild *builder)
{
	Snapshot	result;
	int			XactIsoLevel_save;
	TransactionId xmin_save;

	/*
	 * Fake XactIsoLevel so that the assertions in SnapBuildInitialSnapshot()
	 * don't fire.
	 */
	XactIsoLevel_save = XactIsoLevel;
	XactIsoLevel = XACT_REPEATABLE_READ;

	/*
	 * Likewise, fake MyPgXact->xmin so that the corresponding check passes.
	 */
#if PG_VERSION_NUM >= 140000
	xmin_save = MyProc->xmin;
	MyProc->xmin = InvalidTransactionId;
#else
	xmin_save = MyPgXact->xmin;
	MyPgXact->xmin = InvalidTransactionId;
#endif

	/*
	 * Call the core function to actually build the snapshot.
	 */
	result = SnapBuildInitialSnapshot(builder);

	/*
	 * Restore the original values.
	 */
	XactIsoLevel = XactIsoLevel_save;
#if PG_VERSION_NUM >= 140000
	MyProc->xmin = xmin_save;
#else
	MyPgXact->xmin = xmin_save;
#endif

	return result;
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
process_task_internal(MemoryContext task_cxt)
{
	Name		relschema,
		relname;
	Name		cl_index = NULL;
	Name		rel_tbsp = NULL;
	ArrayType  *ind_tbsps = NULL;
	WorkerTask *task;
	uint32		arr_size;
	TimestampTz start_ts;
	bool		success;
	RangeVar   *relrv;
	Relation	rel;
	Oid			relid;
	ErrorData  *edata;

	task = MyWorkerTask;

	/*
	 * Create the replication slot if there is none. This happens when the
	 * worker is started by the squeeze_table() function, which is run by the
	 * PG executor and therefore cannot build the historic snapshot (due to
	 * the commit 240e0dbacd in PG core). (And the scheduler worker, which
	 * usually creates the slots, is not involved here.)
	 */
	if (task->repl_slot.snap_handle == DSM_HANDLE_INVALID)
		am_i_standalone = true;

	if (am_i_standalone)
	{
		/*
		 * TopMemoryContext is o.k. here, this worker only processes a single
		 * task and then exists.
		 */
		create_replication_slots(1, TopMemoryContext);
		task->repl_slot = squeezeWorkerSlots[0];
	}

	/*
	 * Once the backend sets "in_use" and the worker is launched, only the
	 * worker is expected to change the task, so access it w/o locking.
	 */
	Assert(task->in_use && task->worker_state == WTS_INIT);
	task->worker_state = WTS_IN_PROGRESS;

	relschema = &task->relschema;
	relname = &task->relname;
	if (strlen(NameStr(task->indname)) > 0)
		cl_index = &task->indname;
	if (strlen(NameStr(task->tbspname)) > 0)
		rel_tbsp = &task->tbspname;

	/*
	 * Copy the tablespace mapping array, if one is passed.
	 */
	arr_size = VARSIZE(task->ind_tbsps);
	if (arr_size > 0)
	{
		Assert(arr_size <= IND_TABLESPACES_ARRAY_SIZE);
		ind_tbsps = (ArrayType *) task->ind_tbsps;
	}

	/* Now process the task. */
	ereport(DEBUG1,
			(errmsg("task for table %s.%s is ready for processing",
					NameStr(*relschema), NameStr(*relname))));

	/* Retrieve relid of the table. */
	StartTransactionCommand();
	relrv = makeRangeVar(NameStr(*relschema), NameStr(*relname), -1);
	rel = table_openrv(relrv, AccessShareLock);
	relid = RelationGetRelid(rel);
	table_close(rel, AccessShareLock);
	CommitTransactionCommand();

	/* Declare that this worker takes care of the relation. */
	SpinLockAcquire(&MyWorkerSlot->mutex);
	Assert(MyWorkerSlot->dbid == MyDatabaseId);
	MyWorkerSlot->relid = relid;
	MemSet(&MyWorkerSlot->progress, 0, sizeof(WorkerProgress));
	SpinLockRelease(&MyWorkerSlot->mutex);

	/*
	 * The session origin will be used to mark WAL records produced by the
	 * pg_squeeze extension itself so that they can be skipped easily during
	 * decoding. (We avoid the decoding for performance reasons. Even if those
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
		MemoryContext oldcxt;

		initStringInfo(&query);
		StartTransactionCommand();
		getTypeOutputInfo(TIMESTAMPTZOID, &outfunc, &isvarlena);
		fmgr_info(outfunc, &fmgrinfo);
		start_ts_str = OutputFunctionCall(&fmgrinfo, TimestampTzGetDatum(start_ts));
		/* Make sure the string survives TopTransactionContext. */
		oldcxt = MemoryContextSwitchTo(task_cxt);
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
						 MyWorkerSlot->progress.ins_initial,
						 MyWorkerSlot->progress.ins,
						 MyWorkerSlot->progress.upd,
						 MyWorkerSlot->progress.del);
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

	/* Clear the relid field of this worker's slot. */
	SpinLockAcquire(&MyWorkerSlot->mutex);
	MyWorkerSlot->relid = InvalidOid;
	MemSet(&MyWorkerSlot->progress, 0, sizeof(WorkerProgress));
	SpinLockRelease(&MyWorkerSlot->mutex);
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
VALUES ('%s', '%s', '%s', %s, %s)",
					 NameStr(task->relschema),
					 NameStr(task->relname),
					 unpack_sql_state(edata->sqlerrcode),
					 quote_literal_cstr(edata->message),
					 edata->detail ? quote_literal_cstr(edata->detail) : "''");
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
		SpinLockAcquire(&MyWorkerSlot->mutex);
		MyWorkerSlot->relid = InvalidOid;
		MemSet(&MyWorkerSlot->progress, 0, sizeof(WorkerProgress));
		SpinLockRelease(&MyWorkerSlot->mutex);
	}
}

static void
interrupt_worker(WorkerTask *task)
{
	SpinLockAcquire(&task->mutex);
	/* Do not set if the task exited on its own. */
	if (task->worker_state != WTS_UNUSED)
		task->exit_requested = true;
	SpinLockRelease(&task->mutex);
}

static void
release_task(WorkerTask *task, bool worker)
{
	SpinLockAcquire(&task->mutex);
	if (worker)
	{
		/* Called from squeeze worker (or on its behalf). */
		task->worker_state = WTS_UNUSED;
		Assert(task == MyWorkerTask || MyWorkerTask == NULL);
		task->exit_requested = false;
		/*
		 * The "standalone" worker might have used its private memory for the
		 * snapshot.
		 */
		if (task->repl_slot.snap_private)
		{
			Assert(am_i_standalone);
			/*
			 * Do not call pfree() when holding spinlock. The worker should
			 * only process a single task anyway, so it's not a real leak.
			 */
			task->repl_slot.snap_private = NULL;
		}
		/*
		 * Do not care about detaching from the shared memory:
		 * setup_decoding() runs in a transaction, so the resource owner of
		 * that transaction will take care.
		 */

		MyWorkerTask = NULL;
	}
	else
	{
		/* Called from backend or from the scheduler worker. */
		Assert(task->in_use);
		task->in_use = false;
		if (task->repl_slot.snap_seg)
		{
			dsm_detach(task->repl_slot.snap_seg);
			task->repl_slot.snap_seg = NULL;
			task->repl_slot.snap_handle = DSM_HANDLE_INVALID;
		}
	}
	SpinLockRelease(&task->mutex);
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
	pgstat_report_activity(STATE_IDLE, NULL);
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
