/*---------------------------------------------------------
 *
 * worker.c
 *     Background worker to call functions of pg_squeeze.c
 *
 * Copyright (c) 2016-2018, Cybertec Schönig & Schönig GmbH
 *
 *---------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xact.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#include "pg_squeeze.h"

extern Datum start_worker(PG_FUNCTION_ARGS);

static void start_worker_internal(bool scheduler);

static void worker_sighup(SIGNAL_ARGS);
static void worker_sigterm(SIGNAL_ARGS);

static void scheduler_worker_loop(void);
static void squeeze_worker_loop(void);

static void run_command(char *command);
static int64 get_task_count(void);

/*
 * The function name is ..._worker instead of ..._workers for historic reasons
 * (originally there was only one worker). Is it worth changing?
 */
PG_FUNCTION_INFO_V1(squeeze_start_worker);
Datum
squeeze_start_worker(PG_FUNCTION_ARGS)
{
	/*
	 * The worker eventually runs squeeze_table() function, which in turn
	 * creates a replication slot.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to start squeeze worker"))));

	start_worker_internal(true);
	start_worker_internal(false);

	PG_RETURN_VOID();
}

/*
 * Register either scheduler or squeeze worker, according to the argument.
 */
static void
start_worker_internal(bool scheduler)
{
	WorkerConInteractive	con;
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	char	*kind = scheduler ? "scheduler" : "squeeze";

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
	WorkerConInit	*result;

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
	char	*dbname;
	bool	scheduler;
	char	*kind;

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
						 "WorkerConInteractive is too big" );
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
#if PG_VERSION_NUM >= 110000
	snprintf(worker->bgw_type, BGW_MAXLEN, "squeeze worker");
#endif

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
static int worker_naptime = 20;

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
	Datum	arg;
	Oid	extension_id;
	LOCKTAG		tag;
	LockAcquireResult	lock_res;
	int16	objsubid;
	char	*kind;
	MemoryContext ccxt;

	pqsignal(SIGHUP, worker_sighup);
	pqsignal(SIGTERM, worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Retrieve connection info. */
	Assert(MyBgworkerEntry != NULL);
	arg = MyBgworkerEntry->bgw_main_arg;

	if (MyBgworkerEntry->bgw_main_arg != (Datum) 0)
	{
		WorkerConInit	*con;

		con = (WorkerConInit *) DatumGetPointer(arg);
		am_i_scheduler = con->scheduler;
		BackgroundWorkerInitializeConnection(con->dbname, con->rolename
#if PG_VERSION_NUM >= 110000
											 , 0 /* flags */
#endif
			);
	}
	else
	{
		WorkerConInteractive	con;

		/* Ensure aligned access. */
		memcpy(&con, MyBgworkerEntry->bgw_extra,
			   sizeof(WorkerConInteractive));
		am_i_scheduler = con.scheduler;
		BackgroundWorkerInitializeConnectionByOid(con.dbid, con.roleid
#if PG_VERSION_NUM >= 110000
												  , 0
#endif
			);
	}

	kind = am_i_scheduler ? "scheduler" : "squeeze";

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	extension_id = get_extension_oid("pg_squeeze", false);
	CommitTransactionCommand();

	/*
	 * Concurrent execution of the pg_squeeze functions should not cause data
	 * corruption but those functions are not intended to be run
	 * concurrently. Use extension lock to ensure that at most one worker
	 * exists. (Side effect is that no one should be able to drop the
	 * extension while the worker is running.)
	 *
	 * LockDatabaseObject() would be more convenient, but we'd need to setup
	 * the tag manually elsewhere, to request the lock conditionally. So be
	 * consistent.
	 */
	objsubid = am_i_scheduler ? 0 : 1;
	SET_LOCKTAG_OBJECT(tag, MyDatabaseId, ExtensionRelationId, extension_id,
					   objsubid);
	lock_res = LockAcquire(&tag, ExclusiveLock, true, true);

	if (lock_res == LOCKACQUIRE_NOT_AVAIL)
	{
		elog(WARNING,
			 "one %s worker is already running on %u database",
			 kind, MyDatabaseId);

		proc_exit(0);
	}
	Assert(lock_res == LOCKACQUIRE_OK);

	/*
	 * If either loop encounters an error (which might include SIGINT),
	 * control must not be passed to the background worker loop (bgworker.c)
	 * until our lock has been released. So just catch the error and report it
	 * at lower elevel.
	 */
	ccxt = CurrentMemoryContext;
	PG_TRY();
	{
		if (am_i_scheduler)
			scheduler_worker_loop();
		else
			squeeze_worker_loop();
	}
	PG_CATCH();
	{
		ErrorData  *errdata;
		MemoryContext	oldcxt;

		oldcxt = MemoryContextSwitchTo(ccxt);
		errdata = CopyErrorData();
		if (errdata->message)
			elog(LOG, "%s worker received an error (\"%s\")",
				 kind, errdata->message);
		else
		{
			/* Should not happen, but ... */
			elog(LOG, "%s worker received an error", kind);
		}
		MemoryContextSwitchTo(oldcxt);
	}
	PG_END_TRY();

	LockRelease(&tag, ExclusiveLock, true);

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
	long	delay = 0L;

	while (!got_sigterm)
	{
		int	rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, delay,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ereport(DEBUG1, (errmsg("scheduler worker: checking the schedule")));

		run_command("SELECT squeeze.check_schedule()");

		/* Check later if any table meets the schedule. */
		delay = worker_naptime * 1000L;
	}
}

static void
squeeze_worker_loop(void)
{
	long	delay = 0L;

	while (!got_sigterm)
	{
		int	rc;
		int64	ntasks, i;

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
		run_command("SELECT squeeze.dispatch_new_tasks()");

		/* Process all the tasks currently ready for processing. */
		ntasks = get_task_count();
		ereport(DEBUG1,
				(errmsg("squeeze worker: %zd tasks to process", ntasks)));

		for (i = 0; i < ntasks; i++)
			run_command("SELECT squeeze.process_next_task()");

		/*
		 * Release the replication slot explicitly, ERROR does not ensure
		 * that. (PostgresMain does that for regular backend in the main
		 * loop.)
		 */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();

		/*
		 * Check again if some task is ready for processing, and wait if there
		 * is none.
		 *
		 * Better solution would be to have the scheduler set our latch, but
		 * the background worker API does not provide an easy way for non-core
		 * worker to publish his latch. In particular, the worker code cannot
		 * do anything before the process is forked from the postmaster.
		 */
		ntasks = get_task_count();
		if (ntasks == 0)
			delay = worker_naptime * 1000L;
		else
			delay = 0;
	}
}

/*
 * Run an SQL command that does not return any value.
 */
static void
run_command(char *command)
{
	int	ret;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, command);

	ret = SPI_execute(command, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SELECT command failed: %s", command);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}


/* Return the number pending tasks, i.e. of rows of squeeze.tasks table. */
static int64
get_task_count(void)
{
	int	ret;
	Datum	res_datum;
	bool	isnull;
	int64	result;
	char	*command = "SELECT count(*) FROM squeeze.tasks WHERE state='ready'";
#ifdef USE_ASSERT_CHECKING
	Oid	restype;
#endif

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, command);

	ret = SPI_execute(command, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SELECT command failed: %s", command);

	Assert(SPI_tuptable->tupdesc != NULL);
#ifdef USE_ASSERT_CHECKING
	restype = SPI_gettypeid(SPI_tuptable->tupdesc, 1);
	Assert(restype == INT8OID);
#endif

	Assert(SPI_processed == 1);
	Assert(SPI_tuptable->vals != NULL);
	res_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
							  &isnull);
	Assert(!isnull);

	result = DatumGetInt64(res_datum);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);

	return result;
}
