#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xact.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#include "pg_squeeze.h"

extern Datum start_worker(PG_FUNCTION_ARGS);

static void squeeze_worker_sighup(SIGNAL_ARGS);
static void squeeze_worker_sigterm(SIGNAL_ARGS);

static void run_command(char *command);
static int64 get_task_count(void);

PG_FUNCTION_INFO_V1(squeeze_start_worker);
Datum
squeeze_start_worker(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	/*
	 * The worker eventually runs squeeze_table() function, which in turn
	 * creates a replication slot.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to start squeeze worker"))));

	squeeze_initialize_bgworker(&worker, NULL, MyDatabaseId, GetUserId(),
								MyProcPid);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

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

	PG_RETURN_INT32(pid);
}

void
squeeze_initialize_bgworker(BackgroundWorker *worker,
							bgworker_main_type bgw_main, Oid db, Oid user,
							Oid notify_pid)
{
	char	*c;

	worker->bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker->bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker->bgw_restart_time = BGW_NEVER_RESTART;
	worker->bgw_main = bgw_main;
	sprintf(worker->bgw_library_name, "pg_squeeze");
	sprintf(worker->bgw_function_name, "squeeze_worker_main");
	snprintf(worker->bgw_name, BGW_MAXLEN, "squeeze worker %u", db);
	worker->bgw_main_arg = (Datum) 0;

	/* Store connection info. */
	c = (char *) &worker->bgw_extra;
	memcpy(c, &db, sizeof(Oid));
	c += sizeof(Oid);
	memcpy(c, &user, sizeof(Oid));

	worker->bgw_notify_pid = notify_pid;
}

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

void
squeeze_worker_main(Datum main_arg)
{
	Oid	database_id, user_id, extension_id;
	char	*c;
	LOCKTAG		tag;
	LockAcquireResult	lock_res;
	long	delay;
	int64	ntasks;

	pqsignal(SIGHUP, squeeze_worker_sighup);
	pqsignal(SIGTERM, squeeze_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/*
	 * Retrieve connection info provided by the caller of
	 * squeeze_start_worker().
	 */
	Assert(MyBgworkerEntry != NULL);
	c = MyBgworkerEntry->bgw_extra;
	memcpy(&database_id, c, sizeof(Oid));
	c += sizeof(Oid);
	memcpy(&user_id, c, sizeof(Oid));

	BackgroundWorkerInitializeConnectionByOid(database_id, user_id);

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
	SET_LOCKTAG_OBJECT(tag, MyDatabaseId, ExtensionRelationId, extension_id,
					   0);
	lock_res = LockAcquire(&tag, ExclusiveLock, false, true);

	if (lock_res == LOCKACQUIRE_NOT_AVAIL)
	{
		elog(WARNING,
			 "one squeeze worker is already running on %u database",
			 MyDatabaseId);

		proc_exit(0);
	}
	Assert(lock_res == LOCKACQUIRE_OK);

	delay = 0L;
	ntasks = get_task_count();

	while (!got_sigterm)
	{
		int	rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, delay);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Only try to add rows to "tasks" table if performed enough loops to
		 * process the number we got last time.
		 */
		if (ntasks == 0)
		{
			run_command("SELECT squeeze.add_new_tasks()");
			ntasks = get_task_count();
			elog(DEBUG1, "pg_squeeze: %zd tasks added to queue", ntasks);
		}

		if (ntasks == 0)
		{
			/*
			 * As there's no urgent work, wait some time.
			 *
			 * We might calculate how much time the actual work took and
			 * calculate how long we need to wait so that each iteration
			 * starts exactly N minutes after the previous one. However tables
			 * can have the "task_interval" configured to longer time than 1
			 * minute, so excessive processing time can add to the actual
			 * task_interval anyway. Simply, the task_interval should be
			 * considered the *minimum*.
			 */
			delay = squeeze_worker_naptime * 1000L;
			continue;
		}
		else
		{
			run_command("SELECT squeeze.start_next_task()");

			/* Do the actual work. */
			run_command("SELECT squeeze.process_current_task()");

			/*
			 * Release the replication slot explicitly, ERROR does not ensure
			 * that. (PostgresMain does that for regular backend in the main
			 * loop.)
			 */
			if (MyReplicationSlot != NULL)
				ReplicationSlotRelease();

			/*
			 * We don't know if processing succeeded. This variable should
			 * only minimize the number of calls of get_task_count().
			 */
			ntasks--;

			/*
			 * No reason to wait until ntasks is checked for zero value again.
			 */
			delay = 0L;
		}
	}

	if (!LockRelease(&tag, ExclusiveLock, false))
		elog(ERROR, "Failed to release extension lock");
	proc_exit(0);
}

static void
squeeze_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
squeeze_worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
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
	char	*command = "SELECT count(*) FROM squeeze.tasks";
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
	pgstat_report_activity(STATE_IDLE, NULL);

	return result;
}
