/*-----------------------------------------------------
 *
 * pg_squeeze.c
 *     A tool to eliminate table bloat.
 *
 * Copyright (c) 2016-2023, CYBERTEC PostgreSQL International GmbH
 *
 *-----------------------------------------------------
 */
#include "pg_squeeze.h"

#if PG_VERSION_NUM >= 130000
#include "access/heaptoast.h"
#endif
#include "access/multixact.h"
#include "access/sysattr.h"
#if PG_VERSION_NUM >= 130000
#include "access/toast_internals.h"
#include "access/xlogutils.h"
#endif
#if PG_VERSION_NUM >= 150000
#include "access/xloginsert.h"
#endif
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/toasting.h"
#include "commands/cluster.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "storage/standbydefs.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM < 150000
extern PGDLLIMPORT int wal_segment_size;
extern PGDLLIMPORT bool FirstSnapshotSet;
#endif

#if PG_VERSION_NUM < 120000
#error "PostgreSQL version 12 or higher is required"
#endif

PG_MODULE_MAGIC;

static void squeeze_table_internal(Name relschema, Name relname, Name indname,
								   Name tbspname, ArrayType *ind_tbsp);
static int	index_cat_info_compare(const void *arg1, const void *arg2);

/* Index-to-tablespace mapping. */
typedef struct IndexTablespace
{
	Oid			index;
	Oid			tablespace;
} IndexTablespace;

/* Where should the new table and its indexes be located? */
typedef struct TablespaceInfo
{
	Oid			table;

	int			nindexes;
	IndexTablespace *indexes;
} TablespaceInfo;

/* The WAL segment being decoded. */
XLogSegNo	squeeze_current_segment = 0;

static void check_prerequisites(Relation rel);
static LogicalDecodingContext *setup_decoding(Oid relid, TupleDesc tup_desc,
											  Snapshot *snap_hist);
static void decoding_cleanup(LogicalDecodingContext *ctx);
static CatalogState *get_catalog_state(Oid relid);
static void get_pg_class_info(Oid relid, TransactionId *xmin,
							  Form_pg_class *form_p, TupleDesc *desc_p);
static void get_attribute_info(Oid relid, int relnatts,
							   TransactionId **xmins_p,
							   CatalogState *cat_state);
static void cache_composite_type_info(CatalogState *cat_state, Oid typid);
static void get_composite_type_info(TypeCatInfo *tinfo);
static IndexCatInfo *get_index_info(Oid relid, int *relninds,
									bool *found_invalid,
									bool invalid_check_only,
									bool *found_pk);
static void check_attribute_changes(CatalogState *cat_state);
static void check_index_changes(CatalogState *state);
static void check_composite_type_changes(CatalogState *cat_state);
static void free_catalog_state(CatalogState *state);
static void check_pg_class_changes(CatalogState *state);
static void free_tablespace_info(TablespaceInfo *tbsp_info);
static void resolve_index_tablepaces(TablespaceInfo *tbsp_info,
									 CatalogState *cat_state,
									 ArrayType *ind_tbsp_a);
static void perform_initial_load(Relation rel_src, RangeVar *cluster_idx_rv,
								 Snapshot snap_hist, Relation rel_dst,
								 LogicalDecodingContext *ctx);
static bool has_dropped_attribute(Relation rel);
static Oid	create_transient_table(CatalogState *cat_state, TupleDesc tup_desc,
								   Oid tablespace, Oid relowner);
static Oid *build_transient_indexes(Relation rel_dst, Relation rel_src,
									Oid *indexes_src, int nindexes,
									TablespaceInfo *tbsp_info,
									CatalogState *cat_state,
									LogicalDecodingContext *ctx);
static ScanKey build_identity_key(Oid ident_idx_oid, Relation rel_src,
								  int *nentries);
static bool perform_final_merge(Oid relid_src, Oid *indexes_src, int nindexes,
								Relation rel_dst, ScanKey ident_key,
								int ident_key_nentries,
								IndexInsertState *iistate,
								CatalogState *cat_state,
								LogicalDecodingContext *ctx);
static void swap_relation_files(Oid r1, Oid r2);
static void swap_toast_names(Oid relid1, Oid toastrelid1, Oid relid2,
							 Oid toastrelid2);
#if PG_VERSION_NUM < 130000
static Oid	get_toast_index(Oid toastrelid);
#endif

/*
 * The maximum time to hold AccessExclusiveLock during the final
 * processing. Note that it only process_concurrent_changes() execution time
 * is included here. The very last steps like swap_relation_files() and
 * swap_toast_names() shouldn't get blocked and it'd be wrong to consider them
 * a reason to abort otherwise completed processing.
 */
int			squeeze_max_xlock_time = 0;

/*
 * List of database names for which the background worker should start started
 * during cluster startup. (We require OIDs because there seems to be now good
 * way to pass list of database name w/o adding restrictions on character set
 * characters.)
 */
char	   *squeeze_worker_autostart = NULL;

/*
 * Role on behalf of which automatically-started worker connects to
 * database(s).
 */
char	   *squeeze_worker_role = NULL;

/* The number of squeeze workers per database. */
int			squeeze_workers_per_database = 1;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("pg_squeeze must be loaded via shared_preload_libraries")));

#if PG_VERSION_NUM >= 150000
	squeeze_save_prev_shmem_request_hook();
	shmem_request_hook = squeeze_worker_shmem_request;
#else
	squeeze_worker_shmem_request();
#endif

	squeeze_save_prev_shmem_startup_hook();
	shmem_startup_hook = squeeze_worker_shmem_startup;

	DefineCustomStringVariable(
							   "squeeze.worker_autostart",
							   "Names of databases for which background workers start automatically.",
							   "Comma-separated list for of databases which squeeze worker starts as soon as "
							   "the cluster startup has completed.",
							   &squeeze_worker_autostart,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "squeeze.worker_role",
							   "Role that background workers use to connect to database.",
							   "If background worker was launched automatically on cluster startup, "
							   "it uses this role to initiate database connection(s).",
							   &squeeze_worker_role,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable(
							"squeeze.workers_per_database",
							"Maximum number of squeeze worker processes launched for each database.",
							NULL,
							&squeeze_workers_per_database,
							1, 1, max_worker_processes,
							PGC_POSTMASTER,
							0,

	/*
	 * Assume that the in-core GUC max_worker_processes should already be
	 * assigned and checked before the loading of the modules starts. Since
	 * the context of both this GUC and the max_worker_processes is
	 * PGC_POSTMASTER, no future check should be needed. (Some in-core GUCs
	 * that reference other ones have the hooks despite being PGC_POSTMASTER,
	 * but the reason seems to be that those cannot assume anything about the
	 * order of checking.)
	 */
							NULL, NULL, NULL);

	if (squeeze_worker_autostart)
	{
		List	   *dbnames = NIL;
		char	   *dbname,
				   *c;
		int			len;
		ListCell   *lc;

		if (squeeze_worker_role == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING),
					 (errmsg("\"squeeze.worker_role\" parameter is invalid or not set"))));

		c = squeeze_worker_autostart;
		len = 0;
		dbname = NULL;
		while (true)
		{
			bool		done;

			done = *c == '\0';
			if (done || isspace(*c))
			{
				if (dbname != NULL)
				{
					/* The current item ends here. */
					Assert(len > 0);
					dbnames = lappend(dbnames, pnstrdup(dbname, len));
					dbname = NULL;
					len = 0;
				}

				if (done)
					break;
			}
			else
			{
				/*
				 * Start a new item or add the character to the current one.
				 */
				if (dbname == NULL)
				{
					dbname = c;
					len = 1;
				}
				else
					len++;
			}

			c++;
		}

		if (list_length(dbnames) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 (errmsg("\"squeeze.worker_autostart\" parameter is empty"))));

		foreach(lc, dbnames)
		{
			WorkerConInit *con;
			BackgroundWorker worker;

			dbname = lfirst(lc);

			con = allocate_worker_con_info(dbname, squeeze_worker_role);
			squeeze_initialize_bgworker(&worker, con, NULL, 0);
			RegisterBackgroundWorker(&worker);
		}
		list_free_deep(dbnames);
	}

	DefineCustomIntVariable(
							"squeeze.max_xlock_time",
							"The maximum time the processed table may be locked exclusively.",
							"The source table is locked exclusively during the final stage of "
							"processing. If the lock time should exceed this value, the lock is "
							"released and the final stage is retried a few more times.",
							&squeeze_max_xlock_time,
							0, 0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MS,
							NULL, NULL, NULL);
}

/*
 * The original implementation would certainly fail on PG 16 and higher, due
 * to the commit 240e0dbacd (in the master branch). It's not worth supporting
 * lower versions of pg_squeeze on lower versions of PG server.
 */
extern Datum squeeze_table(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(squeeze_table);
Datum
squeeze_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("the old implementation of the function is no longer supported"),
					errhint("please run \"ALTER EXTENSION pg_squeeze UPDATE\"")));

	PG_RETURN_VOID();
}

/*
 * A substitute for CHECK_FOR_INTERRUPRS.
 *
 * procsignal_sigusr1_handler does not support signaling from a backend to a
 * non-parallel worker (see the values of ProcSignalReason), and an extension
 * has no other way to set the flags checked by ProcessInterrupts(), so the
 * worker cannot use CHECK_FOR_INTERRUPTS. Let's use shared memory to tell the
 * worker that it should exit.  (SIGTERM would terminate the worker easily,
 * but due to race conditions we could terminate another backend / worker
 * which already managed to reuse this worker's PID.)
 */
void
exit_if_requested(void)
{
	bool	exit_requested;

	SpinLockAcquire(&MyWorkerTask->mutex);
	exit_requested = MyWorkerTask->exit_requested;
	SpinLockRelease(&MyWorkerTask->mutex);

	if (!exit_requested)
		return;

	/*
	 * Message similar to that in ProcessInterrupts(), but ERROR is
	 * sufficient here. squeeze_table_impl() should catch it.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating pg_squeeze background worker due to administrator command")));
}


/*
 * Introduced in pg_squeeze 1.6, to be called directly as opposed to calling
 * via the postgres executor.
 *
 * Return true if succeeded. If failed, copy useful information into *edata_p
 * and return false.
 */
bool
squeeze_table_impl(Name relschema, Name relname, Name indname,
				   Name tbspname, ArrayType *ind_tbsp, ErrorData **edata_p,
				   MemoryContext edata_cxt)
{
	bool		result;

	PG_TRY();
	{
		squeeze_table_internal(relschema, relname, indname, tbspname,
							   ind_tbsp);
		result = true;
	}
	PG_CATCH();
	{
		squeeze_handle_error_db(edata_p, edata_cxt);
		result = false;
	}
	PG_END_TRY();

	return result;
}

static void
squeeze_table_internal(Name relschema, Name relname, Name indname,
					   Name tbspname, ArrayType *ind_tbsp)
{
	RangeVar   *relrv_src;
	RangeVar   *relrv_cl_idx = NULL;
	Relation	rel_src,
				rel_dst;
	Oid			rel_src_owner;
	Oid			ident_idx_src,
				ident_idx_dst;
	Oid			relid_src,
				relid_dst;
	Oid			toastrelid_src,
				toastrelid_dst;
	char		replident;
	ScanKey		ident_key;
	int			i,
				ident_key_nentries;
	IndexInsertState *iistate;
	LogicalDecodingContext *ctx;
	ReplicationSlot *slot;
	Snapshot	snap_hist;
	TupleDesc	tup_desc;
	CatalogState *cat_state;
	XLogRecPtr	end_of_wal;
	XLogRecPtr	xlog_insert_ptr;
	int			nindexes;
	Oid		   *indexes_src = NULL,
			   *indexes_dst = NULL;
	bool		invalid_index = false;
	IndexCatInfo *ind_info;
	TablespaceInfo *tbsp_info;
	ObjectAddress object;
	bool		source_finalized;
	bool		xmin_valid;

	relrv_src = makeRangeVar(NameStr(*relschema), NameStr(*relname), -1);
	rel_src = table_openrv(relrv_src, AccessShareLock);

	check_prerequisites(rel_src);

	/*
	 * Retrieve the useful info while holding lock on the relation.
	 */
	ident_idx_src = RelationGetReplicaIndex(rel_src);
	replident = rel_src->rd_rel->relreplident;

	/* The table can have PK although the replica identity is FULL. */
	if (ident_idx_src == InvalidOid && rel_src->rd_pkindex != InvalidOid)
		ident_idx_src = rel_src->rd_pkindex;

	relid_src = RelationGetRelid(rel_src);
	rel_src_owner = RelationGetForm(rel_src)->relowner;
	toastrelid_src = rel_src->rd_rel->reltoastrelid;

	/*
	 * Info to create transient table and to initialize tuplestore we'll use
	 * during logical decoding.
	 */
	tup_desc = CreateTupleDescCopy(RelationGetDescr(rel_src));

	/*
	 * Get ready for the subsequent calls of check_catalog_changes().
	 *
	 * Not all index changes do conflict with the AccessShareLock - see
	 * get_index_info() for explanation.
	 *
	 * XXX It'd still be correct to start the check a bit later, i.e. just
	 * before CreateInitDecodingContext(), but the gain is not worth making
	 * the code less readable.
	 */
	cat_state = get_catalog_state(relid_src);

	/* Give up if it's clear enough to do so. */
	if (cat_state->invalid_index)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("At least one index is invalid"))));

	/*
	 * The relation shouldn't be locked during the call of setup_decoding(),
	 * otherwise another transaction could write XLOG records before the
	 * slots' data.restart_lsn and we'd have to wait for it to finish. If such
	 * a transaction requested exclusive lock on our relation (e.g. ALTER
	 * TABLE), it'd result in a deadlock.
	 *
	 * We can't keep the lock till the end of transaction anyway - that's why
	 * check_catalog_changes() exists.
	 *
	 * XXX Now that the squeeze worker launched by the scheduler worker no
	 * longer needs to call DecodingContextFindStartpoint(), it should not see
	 * running transactions that started before the restart_lsn, so it's
	 * probably no longer necessary to close the relation here. (The worker
	 * launched by the squeeze_table() function does call
	 * DecodingContextFindStartpoint(), however it does so before the current
	 * transaction is started.) Reconsider.
	 */
	table_close(rel_src, AccessShareLock);

	/*
	 * Check if we're ready to capture changes that possibly take place during
	 * the initial load.
	 *
	 * Concurrent DDL causes ERROR in any case, so don't worry about validity
	 * of this test during the next steps.
	 *
	 * Note: we let the plugin do this check on per-change basis, and allow
	 * processing of tables with no identity if only INSERT changes are
	 * decoded. However it seems inconsistent.
	 *
	 * XXX Although ERRCODE_UNIQUE_VIOLATION is no actual "unique violation",
	 * this error code seems to be the best match.
	 * (ERRCODE_TRIGGERED_ACTION_EXCEPTION might be worth consideration as
	 * well.)
	 */
	if (replident == REPLICA_IDENTITY_NOTHING ||
		(replident == REPLICA_IDENTITY_DEFAULT && !OidIsValid(ident_idx_src)))
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Table \"%s\".\"%s\" has no identity index",
						 NameStr(*relschema), NameStr(*relname)))));

	/*
	 * Change processing w/o PK index is not a good idea.
	 *
	 * Note that some users need the "full identity" although the table does
	 * have PK. ("full identity" + UNIQUE constraint is also a valid setup,
	 * but it's harder to check).
	 */
	if (replident == REPLICA_IDENTITY_FULL && !cat_state->have_pk_index)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Replica identity \"full\" not supported"))));

	/*
	 * Clustering index, if any.
	 *
	 * Do not lock the index so far, e.g. just to retrieve OID and to keep it
	 * valid. Neither the relation can be locked continuously, so by keeping
	 * the index locked alone we'd introduce incorrect order of locking.
	 * Although we use only share locks in most cases (so I'm not aware of
	 * particular deadlock scenario), it doesn't seem wise. The worst
	 * consequence of not locking is that perform_initial_load() will error
	 * out.
	 */
	if (indname)
	{
		ereport(DEBUG1,
				(errmsg("clustering index: %s", NameStr(*indname))));

		relrv_cl_idx = makeRangeVar(NameStr(*relschema),
									NameStr(*indname), -1);
	}

	/*
	 * Process tablespace arguments, if provided.
	 *
	 * XXX Currently we consider tablespace DDLs rather infrequent, so we let
	 * such a DDL to break transient table or index creation.  As we can't
	 * keep the source table locked all the time, it's possible for tablespace
	 * to disappear even if it contains the source table. Is it worth locking
	 * the tablespaces here? Since concurrent renaming of a tablespace is
	 * disruptive too, we'd probably need AccessExclusiveLock. Or are such
	 * changes worth making check_catalog_changes() more expensive?
	 */
	tbsp_info = (TablespaceInfo *) palloc0(sizeof(TablespaceInfo));
	if (tbspname)
		tbsp_info->table = get_tablespace_oid(pstrdup(NameStr(*tbspname)),
											  false);
	else
		tbsp_info->table = cat_state->form_class->reltablespace;

	/* Index-to-tablespace mappings. */
	if (ind_tbsp)
		resolve_index_tablepaces(tbsp_info, cat_state, ind_tbsp);

	nindexes = cat_state->relninds;

	/*
	 * Existence of identity index was checked above, so number of indexes and
	 * attributes are both non-zero.
	 */
	Assert(cat_state->form_class->relnatts >= 1);
	Assert(nindexes > 0);

	/* Copy the OIDs into a separate array, for convenient use later. */
	indexes_src = (Oid *) palloc(nindexes * sizeof(Oid));
	for (i = 0; i < nindexes; i++)
		indexes_src[i] = cat_state->indexes[i].oid;

	ctx = setup_decoding(relid_src, tup_desc, &snap_hist);

	relid_dst = create_transient_table(cat_state, tup_desc, tbsp_info->table,
									   rel_src_owner);

	/* The source relation will be needed for the initial load. */
	rel_src = table_open(relid_src, AccessShareLock);

	/*
	 * The new relation should not be visible for other transactions until we
	 * commit, but exclusive lock just makes sense.
	 */
	rel_dst = table_open(relid_dst, AccessExclusiveLock);

	toastrelid_dst = rel_dst->rd_rel->reltoastrelid;

	/*
	 * We need to know whether that no DDL took place that allows for data
	 * inconsistency. The relation was unlocked for some time since last
	 * check, so pass NoLock.
	 */
	check_catalog_changes(cat_state, NoLock);

	/*
	 * This is to satisfy the check introduced by the commit 2776922201f in PG
	 * core. (Per HeapTupleSatisfiesToast() the snapshot shouldn't actually be
	 * used for visibility checks of the TOAST values.)
	 */
	PushActiveSnapshot(snap_hist);

	/*
	 * The historic snapshot is used to retrieve data w/o concurrent changes.
	 */
	perform_initial_load(rel_src, relrv_cl_idx, snap_hist, rel_dst, ctx);

	/*
	 * We no longer need to preserve the rows processed during the initial
	 * load from VACUUM. (User should not run VACUUM on a table that we
	 * currently process, but our stale effective_xmin would also restrict
	 * VACUUM on other tables.)
	 */
	slot = ctx->slot;
	SpinLockAcquire(&slot->mutex);
	xmin_valid = TransactionIdIsValid(slot->effective_xmin);
	slot->effective_xmin = InvalidTransactionId;
	SpinLockRelease(&slot->mutex);

	/*
	 * This should not happen, but it's critical, therefore use ereport()
	 * rather than Assert(). If the value got lost somehow due to releasing
	 * and acquiring the slot, VACUUM could have removed some rows from the
	 * source table that the historic snapshot was still supposed to see.
	 */
	if (!xmin_valid)
		ereport(ERROR,
				(errmsg("effective_xmin of the replication slot \"%s\" is invalid",
						NameStr(slot->data.name))));

	/*
	 * The historic snapshot won't be needed anymore.
	 */
	PopActiveSnapshot();

	/*
	 * This is rather paranoia than anything else --- perform_initial_load()
	 * uses each snapshot to access different table, and it does not cause
	 * catalog changes.
	 */
	InvalidateSystemCaches();

	/*
	 * Check for concurrent changes that would make us stop working later.
	 * Index build can take quite some effort and we don't want to waste it.
	 *
	 * Note: By still holding the share lock we only ensure that the source
	 * relation is not altered underneath index build, but we'll have to
	 * release the lock for a short time at some point. So while we can't
	 * prevent anyone from forcing us to cancel our work, such cancellation
	 * must happen at well-defined moment.
	 */
	check_catalog_changes(cat_state, AccessShareLock);

	/*
	 * Make sure the contents of the transient table is visible for the
	 * scan(s) during index build.
	 */
	CommandCounterIncrement();

	/*
	 * Create indexes on the temporary table - that might take a while.
	 * (Unlike the concurrent changes, which we insert into existing indexes.)
	 */
	PushActiveSnapshot(GetTransactionSnapshot());
	indexes_dst = build_transient_indexes(rel_dst, rel_src, indexes_src,
										  nindexes, tbsp_info, cat_state,
										  ctx);
	PopActiveSnapshot();

	/*
	 * Make the identity index of the transient table visible, for the sake of
	 * concurrent UPDATEs and DELETEs.
	 */
	CommandCounterIncrement();

	/* Tablespace info is no longer needed. */
	free_tablespace_info(tbsp_info);

	/*
	 * Build scan key that we'll use to look for rows to be updated / deleted
	 * during logical decoding.
	 */
	ident_key = build_identity_key(ident_idx_src, rel_src,
								   &ident_key_nentries);

	/*
	 * As we'll need to take exclusive lock later, release the shared one.
	 *
	 * Note: PG core code shouldn't actually participate in such a deadlock,
	 * as it (supposedly) does not raise lock level. Nor should concurrent
	 * call of the squeeze_table() function participate in the deadlock,
	 * because it should have failed much earlier when creating an existing
	 * logical replication slot again. Nevertheless, these circumstances still
	 * don't justify generally bad practice.
	 *
	 * (As we haven't changed the catalog entry yet, there's no need to send
	 * invalidation messages.)
	 */
	table_close(rel_src, AccessShareLock);

	/*
	 * Valid identity index should exist now, see the identity checks above.
	 */
	Assert(OidIsValid(ident_idx_src));

	/* Find "identity index" of the transient relation. */
	ident_idx_dst = InvalidOid;
	for (i = 0; i < nindexes; i++)
	{
		if (ident_idx_src == indexes_src[i])
		{
			ident_idx_dst = indexes_dst[i];
			break;
		}
	}
	if (!OidIsValid(ident_idx_dst))

		/*
		 * Should not happen, concurrent DDLs should have been noticed short
		 * ago.
		 */
		elog(ERROR, "Identity index missing on the transient relation");

	/* Executor state to update indexes. */
	iistate = get_index_insert_state(rel_dst, ident_idx_dst);

	/*
	 * Flush all WAL records inserted so far (possibly except for the last
	 * incomplete page, see GetInsertRecPtr), to minimize the amount of data
	 * we need to flush while holding exclusive lock on the source table.
	 */
	xlog_insert_ptr = GetInsertRecPtr();
	XLogFlush(xlog_insert_ptr);

	/*
	 * Since we'll do some more changes, all the WAL records flushed so far
	 * need to be decoded for sure.
	 */
#if PG_VERSION_NUM >= 150000
	end_of_wal = GetFlushRecPtr(NULL);
#else
	end_of_wal = GetFlushRecPtr();
#endif

	/*
	 * Decode and apply the data changes that occurred while the initial load
	 * was in progress. The XLOG reader should continue where setup_decoding()
	 * has left it.
	 *
	 * Even if the amount of concurrent changes of our source table might not
	 * be significant, both initial load and index build could have produced
	 * many XLOG records that we need to read. Do so before requesting
	 * exclusive lock on the source relation.
	 */
	process_concurrent_changes(ctx, end_of_wal, cat_state, rel_dst,
							   ident_key, ident_key_nentries, iistate,
							   NoLock, NULL);

	/*
	 * This (supposedly cheap) special check should avoid one particular
	 * deadlock scenario: another transaction, performing index DDL
	 * concurrenly (e.g. DROP INDEX CONCURRENTLY) committed change of
	 * indisvalid, indisready, ... and called WaitForLockers() before we
	 * unlocked both source table and its indexes above. WaitForLockers()
	 * waits till the end of the holding (our) transaction as opposed to the
	 * end of our locks, and the other transaction holds (non-exclusive) lock
	 * on both relation and index. In this situation we'd cause deadlock by
	 * requesting exclusive lock. We should recognize this scenario by
	 * checking pg_index alone.
	 */
	ind_info = get_index_info(relid_src, NULL, &invalid_index, true, NULL);
	if (invalid_index)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));
	else
		pfree(ind_info);

	/*
	 * Try a few times to perform the stage that requires exclusive lock on
	 * the source relation.
	 *
	 * XXX Not sure the number of attempts should be configurable. If it fails
	 * several times, admin should either increase squeeze_max_xlock_time or
	 * disable it.
	 */
	source_finalized = false;
	for (i = 0; i < 4; i++)
	{
		if (perform_final_merge(relid_src, indexes_src, nindexes,
								rel_dst, ident_key, ident_key_nentries,
								iistate, cat_state, ctx))
		{
			source_finalized = true;
			break;
		}
		else
			elog(DEBUG1,
				 "pg_squeeze: exclusive lock on table %u had to be released.",
				 relid_src);
	}
	if (!source_finalized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("\"squeeze_max_xlock_time\" prevented squeeze from completion")));

	/*
	 * Done with decoding.
	 *
	 * XXX decoding_cleanup() frees tup_desc, although we've used it not only
	 * for the decoding.
	 */
	decoding_cleanup(ctx);
	ReplicationSlotRelease();

	pfree(ident_key);
	free_index_insert_state(iistate);

	/* The destination table is no longer necessary, so close it. */

	/*
	 * XXX (Should have been closed right after process_concurrent_changes()?)
	 */
	table_close(rel_dst, AccessExclusiveLock);

	/*
	 * Exchange storage (including TOAST) and indexes between the source and
	 * destination tables.
	 */
	swap_relation_files(relid_src, relid_dst);
	CommandCounterIncrement();

	/*
	 * As swap_relation_files() already changed pg_class(reltoastrelid), we
	 * pass toastrelid_dst for relid_src and vice versa.
	 */
	swap_toast_names(relid_src, toastrelid_dst, relid_dst, toastrelid_src);

	for (i = 0; i < nindexes; i++)
		swap_relation_files(indexes_src[i], indexes_dst[i]);
	CommandCounterIncrement();

	if (nindexes > 0)
	{
		pfree(indexes_src);
		pfree(indexes_dst);
	}

	/* State not needed anymore. */
	free_catalog_state(cat_state);

	/*
	 * Drop the transient table including indexes (and possibly constraints on
	 * those indexes).
	 */
	object.classId = RelationRelationId;
	object.objectSubId = 0;
	object.objectId = relid_dst;
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
}

static int
index_cat_info_compare(const void *arg1, const void *arg2)
{
	IndexCatInfo *i1 = (IndexCatInfo *) arg1;
	IndexCatInfo *i2 = (IndexCatInfo *) arg2;

	if (i1->oid > i2->oid)
		return 1;
	else if (i1->oid < i2->oid)
		return -1;
	else
		return 0;
}

/*
 * Raise error if the relation is not eligible for squeezing or any adverse
 * conditions exist.
 *
 * Some of the checks may be redundant (e.g. heap_open() checks relkind) but
 * its safer to have them all listed here.
 */
static void
check_prerequisites(Relation rel)
{
	Form_pg_class form = RelationGetForm(rel);

	/*
	 * The extension is not generic enough to handle AMs other than "heap".
	 */
	if (form->relam != HEAP_TABLE_AM_OID)
		ereport(ERROR,
				(errmsg("pg_squeeze only supports the \"heap\" access method")));

	/* Check the relation first. */
	if (form->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot squeeze partitioned table")));

	if (form->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						RelationGetRelationName(rel))));

	if (form->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a regular table",
						RelationGetRelationName(rel))));

	if (form->relisshared)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is shared relation",
						RelationGetRelationName(rel))));

	if (IsCatalogRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a catalog relation",
						RelationGetRelationName(rel))));

	/*
	 * We cannot simply replace the storage of a mapped relation.
	 *
	 * The previous check should have caught them, but let's try hard to be
	 * safe.
	 */
	if (RelationIsMapped(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is mapped relation",
						RelationGetRelationName(rel))));

	/*
	 * There's no urgent need to process catalog tables.
	 *
	 * Should this limitation be relaxed someday, consider if we need to write
	 * xl_heap_rewrite_mapping records. (Probably not because the whole
	 * "decoding session" takes place within a call of squeeze_table() and our
	 * catalog checks should not allow for a concurrent rewrite that could
	 * make snapmgr.c:tuplecid_data obsolete. Furthermore, such a rewrite
	 * would have to take place before perform_initial_load(), but this is
	 * called before any transactions could have been decoded, so tuplecid
	 * should still be empty anyway.)
	 */
	if (RelationGetRelid(rel) < FirstNormalObjectId)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not user relation",
						RelationGetRelationName(rel))));

	/*
	 * While AFTER trigger should not be an issue (to generate an event must
	 * have got XID assigned, causing setup_decoding() to fail later), open
	 * cursor might be. See comments of the function for details.
	 */
	CheckTableNotInUse(rel, "squeeze_table()");
}

/*
 * Acquire logical replication slot which created either by the scheduler
 * worker or by a backend executing the squeeze_table() function.
 */
static LogicalDecodingContext *
setup_decoding(Oid relid, TupleDesc tup_desc, Snapshot *snap_hist)
{
	ReplSlotStatus	*repl_slot = &MyWorkerTask->repl_slot;
	DecodingOutputState *dstate;
	MemoryContext oldcontext;
	LogicalDecodingContext *ctx;
	XLogRecPtr	restart_lsn;
	dsm_segment *seg = NULL;
	char	*snap_src;

	/*
	 * Use the slot initialized by the scheduler worker (or by the backend
	 * running the squeeze_table() function ).
	 */
	ReplicationSlotAcquire(NameStr(repl_slot->name), true);

	/*
	 * This should not really happen, but if it did, the initial load could
	 * miss some data.
	 */
	if (!TransactionIdIsValid(MyReplicationSlot->effective_xmin))
		ereport(ERROR,
				(errmsg("replication slot \"%s\" has invalid effective_xmin",
						NameStr(repl_slot->name))));

	/*
	 * It's pretty unlikely for some client to have consumed data changes
	 * (accidentally?)  before this worker could acquire the slot, but it's
	 * easy enough to check.
	 */
	if (MyReplicationSlot->data.confirmed_flush != repl_slot->confirmed_flush)
		ereport(ERROR,
				(errmsg("replication slot \"%s\" has incorrect confirm position",
						NameStr(repl_slot->name))));
	/*
	 * Wasn't effective_xmin lost due to releasing and re-acquiring the slot?
	 * (ReplicationSlotRelease() does clear it in some cases. We try to avoid
	 * that, but checking makes sense as this slot field is critical.).
	 */
	if (!TransactionIdIsValid(MyReplicationSlot->effective_xmin))
		ereport(ERROR,
				(errmsg("replication slot \"%s\" has invalid effective_xmin",
						NameStr(MyReplicationSlot->data.name))));

	restart_lsn = MyReplicationSlot->data.restart_lsn;

	/* Restart the decoding context at slot's confirmed_flush */
	ctx = CreateDecodingContext(InvalidXLogRecPtr,
								NIL,
								false,
#if PG_VERSION_NUM >= 130000
								XL_ROUTINE(.page_read = read_local_xlog_page,
										   .segment_open = wal_segment_open,
										   .segment_close = wal_segment_close),
#else
								logical_read_local_xlog_page,
#endif
								NULL, NULL, NULL);

#if PG_VERSION_NUM >= 130000
	/* decode_concurrent_changes() handles the older versions. */
	XLogBeginRead(ctx->reader, MyReplicationSlot->data.restart_lsn);
#endif

	XLByteToSeg(restart_lsn, squeeze_current_segment, wal_segment_size);

	/*
	 * Setup structures to store decoded changes.
	 */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	dstate = palloc0(sizeof(DecodingOutputState));
	dstate->relid = relid;
	dstate->tstore = tuplestore_begin_heap(false, false,
										   maintenance_work_mem);
	dstate->tupdesc = tup_desc;

	/* Initialize the descriptor to store the changes ... */
	dstate->tupdesc_change = CreateTemplateTupleDesc(1);

	TupleDescInitEntry(dstate->tupdesc_change, 1, NULL, BYTEAOID, -1, 0);
	/* ... as well as the corresponding slot. */
	dstate->tsslot = MakeSingleTupleTableSlot(dstate->tupdesc_change,
											  &TTSOpsMinimalTuple);

	dstate->resowner = ResourceOwnerCreate(CurrentResourceOwner,
										   "logical decoding");

	MemoryContextSwitchTo(oldcontext);

	ctx->output_writer_private = dstate;

	/* Retrieve the historic snapshot. */
	if (repl_slot->snap_handle != DSM_HANDLE_INVALID)
	{
		seg = dsm_attach(repl_slot->snap_handle);
		snap_src = (char *) dsm_segment_address(seg);
	}
	else
		snap_src = repl_slot->snap_private;

	*snap_hist = RestoreSnapshot(snap_src);

	if (seg)
		dsm_detach(seg);

	return ctx;
}

static void
decoding_cleanup(LogicalDecodingContext *ctx)
{
	DecodingOutputState *dstate;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	ExecDropSingleTupleTableSlot(dstate->tsslot);
	FreeTupleDesc(dstate->tupdesc_change);
	FreeTupleDesc(dstate->tupdesc);
	tuplestore_end(dstate->tstore);

	FreeDecodingContext(ctx);
}

/*
 * Retrieve the catalog state to be passed later to check_catalog_changes.
 *
 * Caller is supposed to hold (at least) AccessShareLock on the relation.
 */
static CatalogState *
get_catalog_state(Oid relid)
{
	CatalogState *result;

	result = (CatalogState *) palloc0(sizeof(CatalogState));
	result->rel.relid = relid;

	/*
	 * pg_class(xmin) helps to ensure that the "user_catalog_option" wasn't
	 * turned off and on. On the other hand it might restrict some concurrent
	 * DDLs that would be safe as such.
	 */
	get_pg_class_info(relid, &result->rel.xmin, &result->form_class,
					  &result->desc_class);

	result->rel.relnatts = result->form_class->relnatts;

	/*
	 * We might want to avoid the check if relhasindex is false, but
	 * index_update_stats() updates this field in-place. (Currently it should
	 * not change from "true" to "false", but let's be cautious anyway.)
	 */
	result->indexes = get_index_info(relid, &result->relninds,
									 &result->invalid_index, false,
									 &result->have_pk_index);

	/* If any index is "invalid", no more catalog information is needed. */
	if (result->invalid_index)
		return result;

	if (result->form_class->relnatts > 0)
		get_attribute_info(relid, result->form_class->relnatts,
						   &result->rel.attr_xmins, result);

	return result;
}

/*
 * For given relid retrieve pg_class(xmin). Also set *form and *desc if valid
 * pointers are passed.
 */
static void
get_pg_class_info(Oid relid, TransactionId *xmin, Form_pg_class *form_p,
				  TupleDesc *desc_p)
{
	HeapTuple	tuple;
	Form_pg_class form_class;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];

	/*
	 * ScanPgRelation() would do most of the work below, but relcache.c does
	 * not export it.
	 */
	rel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_class_oid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(rel, ClassOidIndexId, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	/*
	 * As the relation might not be locked by some callers, it could have
	 * disappeared.
	 */
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 (errmsg("Table no longer exists"))));
	}

	/* Invalid relfilenode indicates mapped relation. */
	form_class = (Form_pg_class) GETSTRUCT(tuple);
	if (form_class->relfilenode == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 (errmsg("Mapped relation cannot be squeezed"))));

	*xmin = HeapTupleHeaderGetXmin(tuple->t_data);

	if (form_p)
	{
		*form_p = (Form_pg_class) palloc(CLASS_TUPLE_SIZE);
		memcpy(*form_p, form_class, CLASS_TUPLE_SIZE);
	}

	if (desc_p)
		*desc_p = CreateTupleDescCopy(RelationGetDescr(rel));

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * Retrieve array of pg_attribute(xmin) values for given relation, ordered by
 * attnum. (The ordering is not essential but lets us do some extra sanity
 * checks.)
 *
 * If cat_state is passed and the attribute is of a composite type, make sure
 * it's cached in ->comptypes.
 */
static void
get_attribute_info(Oid relid, int relnatts, TransactionId **xmins_p,
				   CatalogState *cat_state)
{
	Relation	rel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	TransactionId *result;
	int			n = 0;

	rel = table_open(AttributeRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	/* System columns should not be ALTERed. */
	ScanKeyInit(&key[1],
				Anum_pg_attribute_attnum,
				BTGreaterStrategyNumber, F_INT2GT,
				Int16GetDatum(0));
	scan = systable_beginscan(rel, AttributeRelidNumIndexId, true, NULL,
							  2, key);
	result = (TransactionId *) palloc(relnatts * sizeof(TransactionId));
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_attribute form;
		int			i;

		Assert(HeapTupleIsValid(tuple));
		form = (Form_pg_attribute) GETSTRUCT(tuple);
		Assert(form->attnum > 0);

		/* AttributeRelidNumIndexId index ensures ordering. */
		i = form->attnum - 1;
		Assert(i == n);

		/*
		 * Caller should hold at least AccesShareLock on the owning relation,
		 * supposedly no need for repalloc(). (elog() rather than Assert() as
		 * it's not difficult to break this assumption during future coding.)
		 */
		if (n++ > relnatts)
			elog(ERROR, "Relation %u has too many attributes", relid);

		result[i] = HeapTupleHeaderGetXmin(tuple->t_data);

		/*
		 * Gather composite type info if needed.
		 */
		if (cat_state != NULL &&
			get_typtype(form->atttypid) == TYPTYPE_COMPOSITE)
			cache_composite_type_info(cat_state, form->atttypid);
	}
	Assert(relnatts == n);
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	*xmins_p = result;
}


/*
 * Make sure that information on a type that caller has recognized as
 * composite type is cached in cat_state.
 */
static void
cache_composite_type_info(CatalogState *cat_state, Oid typid)
{
	int			i;
	bool		found = false;
	TypeCatInfo *tinfo;

	/* Check if we already have this type. */
	for (i = 0; i < cat_state->ncomptypes; i++)
	{
		tinfo = &cat_state->comptypes[i];

		if (tinfo->oid == typid)
		{
			found = true;
			break;
		}
	}

	if (found)
		return;

	/* Extend the comptypes array if necessary. */
	if (cat_state->ncomptypes == cat_state->ncomptypes_max)
	{
		if (cat_state->ncomptypes_max == 0)
		{
			Assert(cat_state->comptypes == NULL);
			cat_state->ncomptypes_max = 2;
			cat_state->comptypes = (TypeCatInfo *)
				palloc(cat_state->ncomptypes_max * sizeof(TypeCatInfo));
		}
		else
		{
			cat_state->ncomptypes_max *= 2;
			cat_state->comptypes = (TypeCatInfo *)
				repalloc(cat_state->comptypes,
						 cat_state->ncomptypes_max * sizeof(TypeCatInfo));
		}
	}

	tinfo = &cat_state->comptypes[cat_state->ncomptypes];
	tinfo->oid = typid;
	get_composite_type_info(tinfo);

	cat_state->ncomptypes++;
}

/*
 * Retrieve information on a type that caller has recognized as composite
 * type. tinfo->oid must be initialized.
 */
static void
get_composite_type_info(TypeCatInfo *tinfo)
{
	Relation	rel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Form_pg_type form_type;
	Form_pg_class form_class;

	Assert(tinfo->oid != InvalidOid);

	/* Find the pg_type tuple. */
	rel = table_open(TypeRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_type_oid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(tinfo->oid));
	scan = systable_beginscan(rel, TypeOidIndexId, true, NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "composite type %u not found", tinfo->oid);

	form_type = (Form_pg_type) GETSTRUCT(tuple);
	Assert(form_type->typtype == TYPTYPE_COMPOSITE);

	/* Initialize the structure. */
	tinfo->xmin = HeapTupleHeaderGetXmin(tuple->t_data);

	/*
	 * Retrieve the pg_class tuple that represents the composite type, as well
	 * as the corresponding pg_attribute tuples.
	 */
	tinfo->rel.relid = form_type->typrelid;
	get_pg_class_info(form_type->typrelid, &tinfo->rel.xmin, &form_class,
					  NULL);
	if (form_class->relnatts > 0)
		get_attribute_info(form_type->typrelid, form_class->relnatts,
						   &tinfo->rel.attr_xmins, NULL);
	else
		tinfo->rel.attr_xmins = NULL;
	tinfo->rel.relnatts = form_class->relnatts;

	pfree(form_class);
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * Retrieve pg_class(oid) and pg_class(xmin) for each index of given
 * relation.
 *
 * If at least one index appears to be problematic in terms of concurrency,
 * *found_invalid receives true and retrieval of index information ends
 * immediately.
 *
 * If invalid_check_only is true, return after having verified that all
 * indexes are valid.
 *
 * Note that some index DDLs can commit while this function is called from
 * get_catalog_state(). If we manage to see these changes, our result includes
 * them and they'll affect the transient table. If any such change gets
 * committed later and we miss it, it'll be identified as disruptive by
 * check_catalog_changes(). After all, there should be no dangerous race
 * conditions.
 */
static IndexCatInfo *
get_index_info(Oid relid, int *relninds, bool *found_invalid,
			   bool invalid_check_only, bool *found_pk)
{
	Relation	rel,
				rel_idx;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	IndexCatInfo *result;
	int			i,
				n = 0;
	int			relninds_max = 4;
	Datum	   *oids_d;
	int16		oidlen;
	bool		oidbyval;
	char		oidalign;
	ArrayType  *oids_a;
	bool		mismatch;

	*found_invalid = false;
	if (found_pk)
		*found_pk = false;

	/*
	 * Open both pg_class and pg_index catalogs at once, so that we have a
	 * consistent view in terms of invalidation. Otherwise we might get
	 * different snapshot for each. Thus, in-progress index changes that do
	 * not conflict with AccessShareLock on the parent table could trigger
	 * false alarms later in check_catalog_changes().
	 */
	rel = table_open(RelationRelationId, AccessShareLock);
	rel_idx = table_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(rel_idx, IndexIndrelidIndexId, true, NULL, 1,
							  key);

	result = (IndexCatInfo *) palloc(relninds_max * sizeof(IndexCatInfo));
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index form;
		IndexCatInfo *res_entry;

		form = (Form_pg_index) GETSTRUCT(tuple);

		/*
		 * First, perform the simple checks that can make the next work
		 * unnecessary.
		 */
		if (!form->indisvalid || !form->indisready || !form->indislive)
		{
			*found_invalid = true;
			break;
		}

		res_entry = (IndexCatInfo *) &result[n++];
		res_entry->oid = form->indexrelid;
		res_entry->xmin = HeapTupleHeaderGetXmin(tuple->t_data);
		if (found_pk && form->indisprimary)
			*found_pk = true;

		/*
		 * Unlike get_attribute_info(), we can't receive the expected number
		 * of entries from caller.
		 */
		if (n == relninds_max)
		{
			relninds_max *= 2;
			result = (IndexCatInfo *)
				repalloc(result, relninds_max * sizeof(IndexCatInfo));
		}
	}
	systable_endscan(scan);
	table_close(rel_idx, AccessShareLock);

	/* Return if invalid index was found or ... */
	if (*found_invalid)
	{
		table_close(rel, AccessShareLock);
		return result;
	}
	/* ... caller is not interested in anything else.  */
	if (invalid_check_only)
	{
		table_close(rel, AccessShareLock);
		return result;
	}

	/*
	 * Enforce sorting by OID, so that the entries match the result of the
	 * following scan using OID index.
	 */
	qsort(result, n, sizeof(IndexCatInfo), index_cat_info_compare);

	if (relninds)
		*relninds = n;
	if (n == 0)
	{
		table_close(rel, AccessShareLock);
		return result;
	}

	/*
	 * Now retrieve the corresponding pg_class(xmax) values.
	 *
	 * Here it seems reasonable to construct an array of OIDs of the pg_class
	 * entries of the indexes and use amsearcharray function of the index.
	 */
	oids_d = (Datum *) palloc(n * sizeof(Datum));
	for (i = 0; i < n; i++)
		oids_d[i] = ObjectIdGetDatum(result[i].oid);
	get_typlenbyvalalign(OIDOID, &oidlen, &oidbyval, &oidalign);
	oids_a = construct_array(oids_d, n, OIDOID, oidlen, oidbyval, oidalign);
	pfree(oids_d);

	ScanKeyInit(&key[0],
				Anum_pg_class_oid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				PointerGetDatum(oids_a));
	key[0].sk_flags |= SK_SEARCHARRAY;
	scan = systable_beginscan(rel, ClassOidIndexId, true, NULL, 1, key);
	i = 0;
	mismatch = false;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		IndexCatInfo *res_item;
		Form_pg_class form_class;
		char	   *namestr;

		if (i == n)
		{
			/* Index added concurrently? */
			mismatch = true;
			break;
		}
		res_item = &result[i++];
		res_item->pg_class_xmin = HeapTupleHeaderGetXmin(tuple->t_data);
		form_class = (Form_pg_class) GETSTRUCT(tuple);
		namestr = NameStr(form_class->relname);
		Assert(strlen(namestr) < NAMEDATALEN);
		strcpy(NameStr(res_item->relname), namestr);

		res_item->reltablespace = form_class->reltablespace;
	}
	if (i < n)
		mismatch = true;

	if (mismatch)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	pfree(oids_a);

	return result;
}


/*
 * Compare the passed catalog information to the info retrieved using the most
 * recent catalog snapshot. Perform the cheapest checks first, the trickier
 * ones later.
 *
 * lock_held is the *least* mode of the lock held by caller on stat->relid
 * relation since the last check. This information helps to avoid unnecessary
 * checks.
 *
 * We check neither constraint nor trigger related DDLs. Since all the
 * concurrent changes we receive from replication slot must have been subject
 * to those constraints / triggers, the transient relation does not need them,
 * and therefore no incompatibility can arise. We only need to make sure that
 * the storage is "compatible", i.e. no column and no index was added /
 * altered / dropped, and no heap rewriting took place.
 *
 * Unlike get_catalog_state(), fresh catalog snapshot is used for each catalog
 * scan. That might increase the chance a little bit that concurrent change
 * will be detected in the current call, instead of the following one.
 *
 * (As long as we use xmin columns of the catalog tables to detect changes, we
 * can't use syscache here.)
 *
 * XXX It's worth checking AlterTableGetLockLevel() each time we adopt a new
 * version of PG core.
 */
void
check_catalog_changes(CatalogState *state, LOCKMODE lock_held)
{
	/*
	 * No DDL should be compatible with this lock mode. (Not sure if this
	 * condition will ever fire.)
	 */
	if (lock_held == AccessExclusiveLock)
		return;

	/*
	 * First the source relation itself.
	 *
	 * Only AccessExclusiveLock guarantees that the pg_class entry hasn't
	 * changed. By lowering this threshold we'd perhaps skip unnecessary check
	 * sometimes (e.g. change of pg_class(relhastriggers) is unimportant), but
	 * we could also miss the check when necessary. It's simply too fragile to
	 * deduce the kind of DDL from lock level, so do this check
	 * unconditionally.
	 */
	check_pg_class_changes(state);

	/*
	 * Index change does not necessarily require lock of the parent relation,
	 * so check indexes unconditionally.
	 */
	check_index_changes(state);

	/*
	 * XXX If any lock level lower than AccessExclusiveLock conflicts with all
	 * commands that change pg_attribute catalog, skip this check if lock_held
	 * is at least that level.
	 */
	check_attribute_changes(state);

	/*
	 * Finally check if any composite type used by the source relation has
	 * changed.
	 */
	if (state->ncomptypes > 0)
		check_composite_type_changes(state);
}

static void
check_pg_class_changes(CatalogState *cat_state)
{
	TransactionId xmin_current;

	get_pg_class_info(cat_state->rel.relid, &xmin_current, NULL, NULL);

	/*
	 * Check if pg_class(xmin) has changed.
	 *
	 * The changes caught here include change of pg_class(relfilenode), which
	 * indicates heap rewriting or TRUNCATE command (or concurrent call of
	 * squeeze_table(), but that should fail to allocate new replication
	 * slot). (Invalid relfilenode does not change, but mapped relations are
	 * excluded from processing by get_catalog_state().)
	 */
	if (!TransactionIdEquals(xmin_current, cat_state->rel.xmin))
		/* XXX Does more suitable error code exist? */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Incompatible DDL or heap rewrite performed concurrently")));
}

/*
 * Check if any tuple of pg_attribute of given relation has changed. In
 * addition, if the attribute type is composite, check for its changes too.
 */
static void
check_attribute_changes(CatalogState *cat_state)
{
	TransactionId *attrs_new;
	int			i;

	/*
	 * Since pg_class should have been checked by now, relnatts can only be
	 * zero if it was zero originally, so there's no info to be compared to
	 * the current state.
	 */
	if (cat_state->rel.relnatts == 0)
	{
		Assert(cat_state->rel.attr_xmins == NULL);
		return;
	}

	/*
	 * Check if any row of pg_attribute changed.
	 *
	 * If the underlying type is composite, pg_attribute(xmin) will not
	 * reflect its change, so pass NULL for cat_state to indicate that we're
	 * not interested in type info at the moment. We'll do that later if all
	 * the cheaper tests pass.
	 */
	get_attribute_info(cat_state->rel.relid, cat_state->rel.relnatts,
					   &attrs_new, NULL);
	for (i = 0; i < cat_state->rel.relnatts; i++)
	{
		if (!TransactionIdEquals(cat_state->rel.attr_xmins[i], attrs_new[i]))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("Table definition changed concurrently")));
	}
	pfree(attrs_new);
}

static void
check_index_changes(CatalogState *cat_state)
{
	IndexCatInfo *inds_new;
	int			relninds_new;
	bool		failed = false;
	bool		invalid_index;
	bool		have_pk_index;

	if (cat_state->relninds == 0)
	{
		Assert(cat_state->indexes == NULL);
		return;
	}

	inds_new = get_index_info(cat_state->rel.relid, &relninds_new,
							  &invalid_index, false, &have_pk_index);

	/*
	 * If this field was set to true, no attention was paid to the other
	 * fields during catalog scans.
	 */
	if (invalid_index)
		failed = true;

	if (!failed && relninds_new != cat_state->relninds)
		failed = true;

	/*
	 * It might be o.k. for the PK index to disappear if the table still has
	 * an unique constraint, but this is too hard to check.
	 */
	if (!failed && cat_state->have_pk_index != have_pk_index)
		failed = true;

	if (!failed)
	{
		int			i;

		for (i = 0; i < cat_state->relninds; i++)
		{
			IndexCatInfo *ind,
					   *ind_new;

			ind = &cat_state->indexes[i];
			ind_new = &inds_new[i];
			if (ind->oid != ind_new->oid ||
				!TransactionIdEquals(ind->xmin, ind_new->xmin) ||
				!TransactionIdEquals(ind->pg_class_xmin,
									 ind_new->pg_class_xmin))
			{
				failed = true;
				break;
			}
		}
	}
	if (failed)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));
	pfree(inds_new);
}

static void
check_composite_type_changes(CatalogState *cat_state)
{
	int			i;
	TypeCatInfo *changed = NULL;

	for (i = 0; i < cat_state->ncomptypes; i++)
	{
		TypeCatInfo *tinfo = &cat_state->comptypes[i];
		TypeCatInfo tinfo_new;
		int			j;

		tinfo_new.oid = tinfo->oid;
		get_composite_type_info(&tinfo_new);

		if (!TransactionIdEquals(tinfo->xmin, tinfo_new.xmin) ||
			!TransactionIdEquals(tinfo->rel.xmin, tinfo_new.rel.xmin) ||
			(tinfo->rel.relnatts != tinfo_new.rel.relnatts))
		{
			changed = tinfo;
			break;
		}

		/*
		 * Check the individual attributes of the type relation.
		 *
		 * This should catch ALTER TYPE ... ALTER ATTRIBUTE ... change of
		 * attribute data type, which is currently not allowed if the type is
		 * referenced by any table. Do it yet in this generic way so that we
		 * don't have to care whether any PG restrictions are relaxed in the
		 * future.
		 */
		for (j = 0; j < tinfo->rel.relnatts; j++)
		{
			if (!TransactionIdEquals(tinfo->rel.attr_xmins[j],
									 tinfo_new.rel.attr_xmins[j]))
			{
				changed = tinfo;
				break;
			}
		}

		if (tinfo_new.rel.relnatts > 0)
		{
			Assert(tinfo_new.rel.attr_xmins != NULL);
			pfree(tinfo_new.rel.attr_xmins);
		}

		if (changed != NULL)
			break;
	}

	if (changed != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of composite type %u detected",
						changed->oid)));
}

static void
free_catalog_state(CatalogState *state)
{
	if (state->form_class)
		pfree(state->form_class);

	if (state->desc_class)
		pfree(state->desc_class);

	if (state->rel.attr_xmins)
		pfree(state->rel.attr_xmins);

	if (state->indexes)
		pfree(state->indexes);

	if (state->comptypes)
	{
		int			i;

		for (i = 0; i < state->ncomptypes; i++)
		{
			TypeCatInfo *tinfo = &state->comptypes[i];

			if (tinfo->rel.attr_xmins)
				pfree(tinfo->rel.attr_xmins);
		}
		pfree(state->comptypes);
	}
	pfree(state);
}

static void
resolve_index_tablepaces(TablespaceInfo *tbsp_info, CatalogState *cat_state,
						 ArrayType *ind_tbsp_a)
{
	int		   *dims,
			   *lb;
	int			i,
				ndim;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elements;
	bool	   *nulls;
	int			nelems,
				nentries;

	/* The CREATE FUNCTION statement should ensure this. */
	Assert(ARR_ELEMTYPE(ind_tbsp_a) == NAMEOID);

	if ((ndim = ARR_NDIM(ind_tbsp_a)) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("Index-to-tablespace mappings must be text[][] array")));

	dims = ARR_DIMS(ind_tbsp_a);
	if (dims[1] != 2)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("The index-to-tablespace mappings must have 2 columns")));

	lb = ARR_LBOUND(ind_tbsp_a);
	for (i = 0; i < ndim; i++)
		if (lb[i] != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("Each dimension of the index-to-tablespace mappings must start at 1")));

	get_typlenbyvalalign(NAMEOID, &elmlen, &elmbyval, &elmalign);
	deconstruct_array(ind_tbsp_a, NAMEOID, elmlen, elmbyval, elmalign,
					  &elements, &nulls, &nelems);
	Assert(nelems % 2 == 0);

	for (i = 0; i < nelems; i++)
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("The index-to-tablespace array is must not contain NULLs")));

	/* Do the actual processing. */
	nentries = nelems / 2;
	tbsp_info->indexes = (IndexTablespace *)
		palloc(nentries * sizeof(IndexTablespace));
	Assert(tbsp_info->nindexes == 0);

	for (i = 0; i < nentries; i++)
	{
		char	   *indname,
				   *tbspname;
		int			j;
		Oid			ind_oid,
					tbsp_oid;
		IndexTablespace *ind_ts;

		/* Find OID of the index. */
		indname = NameStr(*DatumGetName(elements[2 * i]));
		ind_oid = InvalidOid;
		for (j = 0; j < cat_state->relninds; j++)
		{
			IndexCatInfo *ind_cat;

			ind_cat = &cat_state->indexes[j];
			if (strcmp(NameStr(ind_cat->relname), indname) == 0)
			{
				ind_oid = ind_cat->oid;
				break;
			}
		}
		if (!OidIsValid(ind_oid))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("Table has no index \"%s\"", indname)));

		/* Duplicate entries are not expected in the input array. */
		for (j = 0; j < tbsp_info->nindexes; j++)
		{
			ind_ts = &tbsp_info->indexes[j];
			if (ind_ts->index == ind_oid)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("Duplicate tablespace mapping for index \"%s\"",
								indname)));
		}

		/* Look up the tablespace. Fail if it does not exist. */
		tbspname = NameStr(*DatumGetName(elements[2 * i + 1]));
		tbsp_oid = get_tablespace_oid(tbspname, false);

		/* Add the new mapping entry to the array. */
		ind_ts = &tbsp_info->indexes[tbsp_info->nindexes++];
		ind_ts->index = ind_oid;
		ind_ts->tablespace = tbsp_oid;
	}
	pfree(elements);
	pfree(nulls);
}

static void
free_tablespace_info(TablespaceInfo *tbsp_info)
{
	if (tbsp_info->indexes != NULL)
		pfree(tbsp_info->indexes);
	pfree(tbsp_info);
}

/*
 * Use snap_hist snapshot to get the relevant data from rel_src and insert it
 * into rel_dst.
 *
 * Caller is responsible for opening and locking both relations.
 */
static void
perform_initial_load(Relation rel_src, RangeVar *cluster_idx_rv,
					 Snapshot snap_hist, Relation rel_dst,
					 LogicalDecodingContext *ctx)
{
	bool		use_sort;
	int			batch_size,
				batch_max_size;
	Size		tuple_array_size;
	bool		tuple_array_can_expand = true;
	Tuplesortstate *tuplesort = NULL;
	Relation	cluster_idx = NULL;
	TableScanDesc heap_scan = NULL;
	TupleTableSlot *slot;
	IndexScanDesc index_scan = NULL;
	HeapTuple  *tuples = NULL;
	ResourceOwner res_owner_old,
				res_owner_plan;
	BulkInsertState bistate;
	MemoryContext load_cxt,
				old_cxt;
	XLogRecPtr	end_of_wal_prev = InvalidXLogRecPtr;
	DecodingOutputState *dstate;
	bool	has_dropped_attr;
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];


	/*
	 * Also remember that the WAL records created during the load should not
	 * be decoded later.
	 */
	dstate = (DecodingOutputState *) ctx->output_writer_private;
	dstate->rorigin = replorigin_session_origin;

	if (cluster_idx_rv != NULL)
	{
		cluster_idx = relation_openrv(cluster_idx_rv, AccessShareLock);

		/*
		 * Use the cluster.c API to check if the index can be used for
		 * clustering.
		 */
#if PG_VERSION_NUM >= 150000
		check_index_is_clusterable(rel_src, RelationGetRelid(cluster_idx),
								   NoLock);
#else
		check_index_is_clusterable(rel_src, RelationGetRelid(cluster_idx),
								   false, NoLock);
#endif

		/*
		 * Decide whether index scan or explicit sort should be used.
		 *
		 * Caller does not expect to see any additional locks, so use a
		 * separate resource owner to keep track of them.
		 */
		res_owner_old = CurrentResourceOwner;
		res_owner_plan = ResourceOwnerCreate(res_owner_old,
											 "use_sort owner");
		CurrentResourceOwner = res_owner_plan;
		use_sort = plan_cluster_use_sort(rel_src->rd_id, cluster_idx->rd_id);

		/*
		 * Now use the special resource owner to release those planner locks.
		 * In fact this owner should contain any other resources, that the
		 * planner might have allocated. Release them all, to avoid leak.
		 */
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_LOCKS, false, false);
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS, false, false);

		/* Cleanup. */
		CurrentResourceOwner = res_owner_old;
		ResourceOwnerDelete(res_owner_plan);
	}
	else
		use_sort = false;

	if (use_sort || cluster_idx == NULL)
		heap_scan = table_beginscan(rel_src, snap_hist, 0, (ScanKey) NULL);
	else
	{
		index_scan = index_beginscan(rel_src, cluster_idx, snap_hist, 0, 0);
		index_rescan(index_scan, NULL, 0, NULL, 0);
	}

	slot = table_slot_create(rel_src, NULL);

	if (use_sort)
		tuplesort = tuplesort_begin_cluster(RelationGetDescr(rel_src),
											cluster_idx,
											maintenance_work_mem,
											NULL,
											false);

	/*
	 * If tuplesort is not applicable, we store as much data as we can store
	 * in memory. The more memory is available, the fewer iterations.
	 */
	if (!use_sort)
	{
		batch_max_size = 1024;
		tuple_array_size = batch_max_size * sizeof(HeapTuple);
		/* The minimum value of maintenance_work_mem is 1024 kB. */
		Assert(tuple_array_size / 1024 < maintenance_work_mem);
		tuples = (HeapTuple *) palloc(tuple_array_size);
	}

	/* Expect many insertions. */
	bistate = GetBulkInsertState();

	/* Has the relation at least one dropped attribute? */
	has_dropped_attr = has_dropped_attribute(rel_src);

	/*
	 * The processing can take many iterations. In case any data manipulation
	 * below leaked, try to defend against out-of-memory conditions by using a
	 * separate memory context.
	 */
	load_cxt = AllocSetContextCreate(CurrentMemoryContext,
									 "pg_squeeze initial load cxt",
									 ALLOCSET_DEFAULT_SIZES);
	old_cxt = MemoryContextSwitchTo(load_cxt);

	while (true)
	{
		HeapTuple	tup_in = NULL;
		int			i;
		Size		data_size = 0;
		XLogRecPtr	end_of_wal;

		/* Sorting cannot be split into batches. */
		for (i = 0;; i++)
		{
			bool		have_tup_copy = false;

			/*
			 * While tuplesort is responsible for not exceeding
			 * maintenance_work_mem itself, we must check if the tuple array
			 * does.
			 *
			 * Since the tuple cannot be put back to the scan, it'd make
			 * things tricky if we involved the current tuple in the
			 * computation. Since the unit of maintenance_work_mem is kB, one
			 * extra tuple shouldn't hurt too much.
			 */
			if (!use_sort && ((data_size + tuple_array_size) / 1024)
				>= maintenance_work_mem)
			{
				/*
				 * data_size should still be zero if tup_in is the first item
				 * of the current batch and the array itself should never
				 * exceed maintenance_work_mem. XXX If the condition above is
				 * changed to include the current tuple (i.e. we put the
				 * current tuple aside for the next batch), make sure the
				 * first tuple of a batch is inserted regardless its size. We
				 * cannot shrink the array in favor of actual data in generic
				 * case (i.e. tuple size can in general be bigger than
				 * maintenance_work_mem).
				 */
				Assert(i > 0);

				break;
			}

			/*
			 * Perform the tuple retrieval in the original context so that no
			 * scan data is freed during the cleanup between batches.
			 */
			MemoryContextSwitchTo(old_cxt);
			{
				bool		res;

				if (use_sort || cluster_idx == NULL)
					res = table_scan_getnextslot(heap_scan,
												 ForwardScanDirection,
												 slot);
				else
					res = index_getnext_slot(index_scan,
											 ForwardScanDirection,
											 slot);

				if (res)
				{
					bool		shouldFree;

					tup_in = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
					/* TTSOpsBufferHeapTuple has .get_heap_tuple != NULL. */
					Assert(!shouldFree);
				}
				else
					tup_in = NULL;
			}
			MemoryContextSwitchTo(load_cxt);

			/*
			 * Ran out of input data?
			 */
			if (tup_in == NULL)
				break;

			/* Flatten the tuple if needed. */
			if (HeapTupleHasExternal(tup_in))
			{
				tup_in = toast_flatten_tuple(tup_in,
											 RelationGetDescr(rel_src));
				have_tup_copy = true;
			}

			/*
			 * If at least one attribute has been dropped, we need to deform /
			 * form the tuple to make sure that set the values of the dropped
			 * attribute(s) are NULL. (Unfortunately we don't know if the
			 * table was already squeezed since the last ALTER TABLE ... DROP
			 * COLUMN ... command.)
			 */
			if (has_dropped_attr)
			{
				HeapTuple	tup_orig = tup_in;
				TupleDesc	tup_desc = RelationGetDescr(rel_src);

				heap_deform_tuple(tup_in, tup_desc, values, isnull);

				for (int j = 0; j < tup_desc->natts; j++)
				{
					if (TupleDescAttr(tup_desc, j)->attisdropped)
						isnull[j] = true;
				}

				tup_in = heap_form_tuple(tup_desc, values, isnull);
				if (have_tup_copy)
					/* tup_in is a flat copy. We do not want two copies. */
					heap_freetuple(tup_orig);
				have_tup_copy = true;
			}

			if (use_sort)
			{
				tuplesort_putheaptuple(tuplesort, tup_in);
				/* tuplesort should have copied the tuple. */
				if (have_tup_copy)
					heap_freetuple(tup_in);
			}
			else
			{
				exit_if_requested();

				/*
				 * Check for a free slot early enough so that the current
				 * tuple can be stored even if the array cannot be
				 * reallocated. Do not try again and again if the tuple array
				 * reached the maximum value.
				 */
				if (i == (batch_max_size - 1) && tuple_array_can_expand)
				{
					int			batch_max_size_new;
					Size		tuple_array_size_new;

					batch_max_size_new = 2 * batch_max_size;
					tuple_array_size_new = batch_max_size_new *
						sizeof(HeapTuple);

					/*
					 * Besides being of valid size, the new array should allow
					 * for storing some data w/o exceeding
					 * maintenance_work_mem. Check also batch_max_size_new for
					 * overflow although AllocSizeIsValid() probably should
					 * detect a problem much earlier. XXX Consider tuning the
					 * portion of maintenance_work_mem that the array can use.
					 */
					if (!AllocSizeIsValid(tuple_array_size_new) ||
						batch_max_size_new < 0 ||
						tuple_array_size_new / 1024 >=
						maintenance_work_mem / 16)
						tuple_array_can_expand = false;

					/*
					 * Only expand the array if the current iteration does not
					 * violate maintenance_work_mem.
					 */
					if (tuple_array_can_expand)
					{
						tuples = (HeapTuple *)
							repalloc(tuples, tuple_array_size_new);

						batch_max_size = batch_max_size_new;
						tuple_array_size = tuple_array_size_new;
					}
				}

				if (!have_tup_copy)
					tup_in = heap_copytuple(tup_in);

				/*
				 * Store the tuple and account for its size.
				 */
				tuples[i] = tup_in;
				data_size += HEAPTUPLESIZE + tup_in->t_len;

				/*
				 * If the tuple array could not be expanded, stop reading for
				 * the current batch.
				 */
				if (i == (batch_max_size - 1))
				{
					/* The current tuple belongs to the current batch. */
					i++;

					break;
				}
			}
		}

		/*
		 * Insert the tuples into the target table.
		 *
		 * check_catalog_changes() shouldn't be necessary as long as the
		 * AccessSqhareLock we hold on the source relation does not allow
		 * change of table type. (Should ALTER INDEX take place concurrently,
		 * it does not break the heap insertions. In such a case we'll find
		 * out later that we need to terminate processing of the current
		 * table, but it's probably not worth checking each batch.)
		 */

		if (use_sort)
			tuplesort_performsort(tuplesort);
		else
		{
			/*
			 * Has the previous batch processed all the remaining tuples?
			 *
			 * In theory, the counter might end up zero as a result of
			 * overflow. However in practice 'i' should not overflow because
			 * its upper limit is controlled by 'batch_max_size' which is also
			 * of the int data type, and which in turn should not overflow
			 * because value much lower than INT_MAX will make
			 * AllocSizeIsValid(tuple_array_size_new) return false.
			 */
			if (i == 0)
				break;
		}

		batch_size = i;
		i = 0;
		while (true)
		{
			HeapTuple	tup_out;

			exit_if_requested();

			if (use_sort)
				tup_out = tuplesort_getheaptuple(tuplesort, true);
			else
			{
				if (i == batch_size)
					tup_out = NULL;
				else
					tup_out = tuples[i++];
			}

			if (tup_out == NULL)
				break;

			/*
			 * Insert the tuple into the new table.
			 *
			 * XXX Should this happen outside load_cxt? Currently "bistate" is
			 * a flat object (i.e. it does not point to any memory chunk that
			 * the previous call of heap_insert() might have allocated) and
			 * thus the cleanup between batches should not damage it, but
			 * can't it get more complex in future PG versions?
			 */
			heap_insert(rel_dst, tup_out, GetCurrentCommandId(true), 0,
						bistate);

			/* Update the progress information. */
			SpinLockAcquire(&MyWorkerSlot->mutex);
			MyWorkerSlot->progress.ins_initial += 1;
			SpinLockRelease(&MyWorkerSlot->mutex);

			if (!use_sort)
				pfree(tup_out);
		}

		/*
		 * Reached the end of scan when retrieving data from heap or index?
		 */
		if (tup_in == NULL)
			break;

		/*
		 * Free possibly-leaked memory.
		 */
		MemoryContextReset(load_cxt);

		/*
		 * Decode the WAL produced by the load, as well as by other
		 * transactions, so that the replication slot can advance and WAL does
		 * not pile up. Of course we must not apply the changes until the
		 * initial load has completed.
		 *
		 * Note that the insertions into the new table shouldn't actually be
		 * decoded, they should be filtered out by their origin.
		 */
#if PG_VERSION_NUM >= 150000
		end_of_wal = GetFlushRecPtr(NULL);
#else
		end_of_wal = GetFlushRecPtr();
#endif
		if (end_of_wal > end_of_wal_prev)
		{
			MemoryContextSwitchTo(old_cxt);
			decode_concurrent_changes(ctx, end_of_wal, NULL);
			MemoryContextSwitchTo(load_cxt);
		}
		end_of_wal_prev = end_of_wal;
	}

	/*
	 * At whichever stage the loop broke, the historic snapshot should no
	 * longer be active.
	 */

	/* Cleanup. */
	FreeBulkInsertState(bistate);

	if (use_sort)
		tuplesort_end(tuplesort);
	else
		pfree(tuples);

	if (heap_scan != NULL)
		table_endscan(heap_scan);

	if (index_scan != NULL)
		index_endscan(index_scan);

	ExecDropSingleTupleTableSlot(slot);

	/*
	 * Unlock the index, but not the relation yet - caller will do so when
	 * appropriate.
	 */
	if (cluster_idx != NULL)
		relation_close(cluster_idx, AccessShareLock);

	MemoryContextSwitchTo(old_cxt);
	MemoryContextDelete(load_cxt);

	elog(DEBUG1, "pg_squeeze: the initial load completed");
}

/*
 * Check if relation has at least one dropped attribute.
 */
static bool
has_dropped_attribute(Relation rel)
{
	TupleDesc	tup_desc = RelationGetDescr(rel);

	for (int i = 0; i < tup_desc->natts; i++)
	{
		Form_pg_attribute attr = &tup_desc->attrs[i];

		if (attr->attisdropped)
			return true;
	}

	return false;
}

/*
 * Create a table into which we'll copy the contents of the source table, as
 * well as changes of the source table that happened during the copying. At
 * the end of processing we'll just swap storage of the transient and the
 * source relation and drop the transient one.
 *
 * Return oid of the new relation, which is neither locked nor open.
 */
static Oid
create_transient_table(CatalogState *cat_state, TupleDesc tup_desc,
					   Oid tablespace, Oid relowner)
{
	StringInfo	relname;
	Form_pg_class form_class;
	HeapTuple	tuple;
	Datum		reloptions;
	bool		isnull;
	Oid			toastrelid;
	Oid			result;

	/* As elsewhere in PG core. */
	if (OidIsValid(tablespace) && tablespace != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		/*
		 * squeeze_table() must be executed by superuser because it creates
		 * and drops the replication slot. However it should not be a way to
		 * do things that the table owner is not allowed to. (For indexes we
		 * assume they all have the same owner as the table.)
		 */
#if PG_VERSION_NUM >= 160000
		aclresult = object_aclcheck(TableSpaceRelationId, tablespace,
									relowner, ACL_CREATE);
#else
		aclresult = pg_tablespace_aclcheck(tablespace, relowner, ACL_CREATE);
#endif
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult,
						   OBJECT_TABLESPACE,
						   get_tablespace_name(tablespace));
	}
	if (tablespace == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	relname = makeStringInfo();
	appendStringInfo(relname, "tmp_%u", cat_state->rel.relid);

	/*
	 * Constraints are not created because each data change must be committed
	 * in the source table before we see it during initial load or via logical
	 * decoding.
	 *
	 * Values of some arguments (e.g. oidislocal, oidinhcount) are unimportant
	 * since the transient table and its catalog entries will eventually get
	 * dropped. On the other hand, we do not change catalog regarding the
	 * source relation.
	 */
	form_class = cat_state->form_class;

	/*
	 * reloptions must be preserved, so fetch them from the catalog.
	 */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(cat_state->rel.relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 cat_state->rel.relid);
	reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								 &isnull);
	Assert(!isnull || reloptions == (Datum) 0);

	result = heap_create_with_catalog(relname->data,
									  form_class->relnamespace,
									  tablespace,
									  InvalidOid,
									  InvalidOid,
									  InvalidOid,
									  form_class->relowner,
									  form_class->relam,
									  tup_desc,
									  NIL,
									  form_class->relkind,
									  form_class->relpersistence,
									  false,
									  false,
									  ONCOMMIT_NOOP,
									  reloptions,
									  false,
									  false,
									  false,
									  InvalidOid,	/* relrewrite */
									  NULL);

	Assert(OidIsValid(result));

	ReleaseSysCache(tuple);

	elog(DEBUG1, "pg_squeeze: transient relation created: %u", result);

	/* Make sure the transient relation is visible.  */
	CommandCounterIncrement();

	/*
	 * See cluster.c:make_new_heap() for details about the supposed
	 * (non)existence of TOAST relation on both source and the transient
	 * relations.
	 */
	toastrelid = form_class->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		/* keep the existing toast table's reloptions, if any */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastrelid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", toastrelid);
		reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
									 &isnull);
		Assert(!isnull || reloptions == (Datum) 0);

		/*
		 * No lock is needed on the target relation since no other transaction
		 * should be able to see it until our transaction commits. However,
		 * table_open() is eventually called and that would cause assertion
		 * failure if we passed NoLock. We can pass any other lock mode.
		 *
		 * On PG versions: the OIDOldToast argument was added to
		 * NewHeapCreateToastTable() in v14, but the change was back-patched
		 * to the minor releases down to v11. Regarding the value of
		 * OIDOldToast, it does not matter here because the auxiliary table
		 * will eventually be dropped.
		 */
#if (PG_VERSION_NUM >= 140000) || \
	(PG_VERSION_NUM < 140000 && PG_VERSION_NUM > 130004) || \
	(PG_VERSION_NUM < 130000 && PG_VERSION_NUM > 120008)
		NewHeapCreateToastTable(result, reloptions, AccessExclusiveLock,
								InvalidOid);
#else
		NewHeapCreateToastTable(result, reloptions, AccessExclusiveLock);
#endif

		ReleaseSysCache(tuple);

		/* Make sure the TOAST relation is visible.  */
		CommandCounterIncrement();
	}

	return result;
}

/*
 * Make sure "dst" relation has the same indexes as "src".
 *
 * indexes_src is array of existing indexes on the source relation and
 * nindexes the number of its entries.
 *
 * An array of oids of corresponding indexes created on the destination
 * relation is returned. The order of items does match, so we can use these
 * arrays to swap index storage.
 */
static Oid *
build_transient_indexes(Relation rel_dst, Relation rel_src,
						Oid *indexes_src, int nindexes,
						TablespaceInfo *tbsp_info, CatalogState *cat_state,
						LogicalDecodingContext *ctx)
{
	StringInfo	ind_name;
	int			i;
	Oid		   *result;
	XLogRecPtr	end_of_wal_prev = InvalidXLogRecPtr;

	Assert(nindexes > 0);

	ind_name = makeStringInfo();
	result = (Oid *) palloc(nindexes * sizeof(Oid));

	for (i = 0; i < nindexes; i++)
	{
		Oid			ind_oid,
					ind_oid_new,
					tbsp_oid;
		Relation	ind;
		IndexInfo  *ind_info;
		int			j,
					heap_col_id;
		List	   *colnames;
		int16		indnatts;
		Oid		   *collations,
				   *opclasses;
		HeapTuple	tup;
		bool		isnull;
		Datum		d;
		oidvector  *oidvec;
		int2vector *int2vec;
		size_t		oid_arr_size;
		size_t		int2_arr_size;
		int16	   *indoptions;
		text	   *reloptions = NULL;
		bits16		flags;
		XLogRecPtr	end_of_wal;
#if PG_VERSION_NUM >= 170000
		Datum		*opclassOptions;
#endif

		ind_oid = indexes_src[i];
		ind = index_open(ind_oid, AccessShareLock);
		ind_info = BuildIndexInfo(ind);

		/*
		 * Tablespace defaults to the original one, but can be overridden by
		 * tbsp_info.
		 */
		tbsp_oid = InvalidOid;
		for (j = 0; j < tbsp_info->nindexes; j++)
		{
			IndexTablespace *ind_tbsp;

			ind_tbsp = &tbsp_info->indexes[j];
			if (ind_tbsp->index == ind_oid)
			{
				tbsp_oid = ind_tbsp->tablespace;
				break;
			}
		}

		if (tbsp_oid == GLOBALTABLESPACE_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only shared relations can be placed in pg_global tablespace")));

		if (!OidIsValid(tbsp_oid))
		{
			bool		found = false;

			for (j = 0; j < cat_state->relninds; j++)
			{
				IndexCatInfo *ind_cat;

				ind_cat = &cat_state->indexes[j];
				if (ind_cat->oid == ind_oid)
				{
					tbsp_oid = ind_cat->reltablespace;
					found = true;
					break;
				}
			}

			/*
			 * It's o.k. for tbsp_oid to end up invalid (if the default
			 * tablespace of the database should be used), but the index
			 * shouldn't have disappeared (caller should hold share lock on
			 * the relation).
			 */
			if (!found)
				elog(ERROR, "Failed to retrieve index tablespace");
		}

		/*
		 * Index name really doesn't matter, we'll eventually use only their
		 * storage. Just make them unique within the table.
		 */
		resetStringInfo(ind_name);
		appendStringInfo(ind_name, "ind_%d", i);

		flags = 0;
		if (ind->rd_index->indisprimary)
			flags |= INDEX_CREATE_IS_PRIMARY;

		colnames = NIL;
		indnatts = ind->rd_index->indnatts;
		oid_arr_size = sizeof(Oid) * indnatts;
		int2_arr_size = sizeof(int16) * indnatts;

		collations = (Oid *) palloc(oid_arr_size);
		for (j = 0; j < indnatts; j++)
		{
			char	   *colname;

			heap_col_id = ind->rd_index->indkey.values[j];
			if (heap_col_id > 0)
			{
				Form_pg_attribute att;

				/* Normal attribute. */
				att = TupleDescAttr(rel_src->rd_att, heap_col_id - 1);
				colname = pstrdup(NameStr(att->attname));
				collations[j] = att->attcollation;
			}
			else if (heap_col_id == 0)
			{
				HeapTuple	tuple;
				Form_pg_attribute att;

				/*
				 * Expression column is not present in relcache. What we need
				 * here is an attribute of the *index* relation.
				 */
				tuple = SearchSysCache2(ATTNUM,
										ObjectIdGetDatum(ind_oid),
										Int16GetDatum(j + 1));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR,
						 "cache lookup failed for attribute %d of relation %u",
						 j + 1, ind_oid);
				att = (Form_pg_attribute) GETSTRUCT(tuple);
				colname = pstrdup(NameStr(att->attname));
				collations[j] = att->attcollation;
				ReleaseSysCache(tuple);
			}
			else
				elog(ERROR, "Unexpected column number: %d",
					 heap_col_id);

			colnames = lappend(colnames, colname);
		}

		/*
		 * Special effort needed for variable length attributes of
		 * Form_pg_index.
		 */
		tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(ind_oid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for index %u", ind_oid);
		d = SysCacheGetAttr(INDEXRELID, tup, Anum_pg_index_indclass, &isnull);
		Assert(!isnull);
		oidvec = (oidvector *) DatumGetPointer(d);
		opclasses = (Oid *) palloc(oid_arr_size);
		memcpy(opclasses, oidvec->values, oid_arr_size);

		d = SysCacheGetAttr(INDEXRELID, tup, Anum_pg_index_indoption,
							&isnull);
		Assert(!isnull);
		int2vec = (int2vector *) DatumGetPointer(d);
		indoptions = (int16 *) palloc(int2_arr_size);
		memcpy(indoptions, int2vec->values, int2_arr_size);
		ReleaseSysCache(tup);

		tup = SearchSysCache1(RELOID, ObjectIdGetDatum(ind_oid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for index relation %u", ind_oid);
		d = SysCacheGetAttr(RELOID, tup, Anum_pg_class_reloptions, &isnull);
		reloptions = !isnull ? DatumGetTextPCopy(d) : NULL;
		ReleaseSysCache(tup);

#if PG_VERSION_NUM >= 170000
		opclassOptions = palloc0(sizeof(Datum) * ind_info->ii_NumIndexAttrs);
		for (j = 0; j < ind_info->ii_NumIndexAttrs; j++)
			opclassOptions[j] = get_attoptions(ind_oid, j + 1);
#endif

		/*
		 * Publish information on what we're going to do. This is especially
		 * important if parallel workers are used to build the index.
		 */
		debug_query_string = "pg_squeeze index build";

		/*
		 * Neither parentIndexRelid nor parentConstraintId needs to be passed
		 * since the new catalog entries (pg_constraint, pg_inherits) will
		 * eventually be dropped. Therefore there's no need to record valid
		 * dependency on parents.
		 */
		ind_oid_new = index_create(rel_dst,
								   ind_name->data,
								   InvalidOid,
								   InvalidOid,	/* parentIndexRelid */
								   InvalidOid,	/* parentConstraintId */
								   InvalidOid,
								   ind_info,
								   colnames,
								   ind->rd_rel->relam,
								   tbsp_oid,
								   collations,
								   opclasses,
#if PG_VERSION_NUM >= 170000
								   opclassOptions,
#endif
								   indoptions,
#if PG_VERSION_NUM >= 170000
								   /*
									* stattargets not needed for the transient
									* index, the value of the source index
									* will remain (we only swap the storage).
									*/
								   NULL,
#endif
								   PointerGetDatum(reloptions),
								   flags,	/* flags */
								   0,	/* constr_flags */
								   false,	/* allow_system_table_mods */
								   false,	/* is_internal */
								   NULL /* constraintId */
			);
		result[i] = ind_oid_new;

		debug_query_string = NULL;

		index_close(ind, AccessShareLock);
		list_free_deep(colnames);
		pfree(collations);
		pfree(opclasses);
		pfree(indoptions);
		if (reloptions)
			pfree(reloptions);

		/*
		 * Like in perform_initial_load(), process some WAL so that the
		 * segment files can be recycled. Unlike the initial load, do not set
		 * replorigin_session_origin because index changes are not decoded
		 * anyway.
		 */
#if PG_VERSION_NUM >= 150000
		end_of_wal = GetFlushRecPtr(NULL);
#else
		end_of_wal = GetFlushRecPtr();
#endif
		if (end_of_wal > end_of_wal_prev)
			decode_concurrent_changes(ctx, end_of_wal, NULL);
		end_of_wal_prev = end_of_wal;
	}

	return result;
}

/*
 * Build scan key to process logical changes.
 *
 * Caller must hold at least AccessShareLock on rel_src.
 */
static ScanKey
build_identity_key(Oid ident_idx_oid, Relation rel_src, int *nentries)
{
	Relation	ident_idx_rel;
	Form_pg_index ident_idx;
	int			n,
				i;
	ScanKey		result;

	Assert(OidIsValid(ident_idx_oid));
	ident_idx_rel = index_open(ident_idx_oid, AccessShareLock);
	ident_idx = ident_idx_rel->rd_index;
	n = ident_idx->indnatts;
	result = (ScanKey) palloc(sizeof(ScanKeyData) * n);
	for (i = 0; i < n; i++)
	{
		ScanKey		entry;
		int16		relattno;
		Form_pg_attribute att;
		Oid			opfamily,
					opcintype,
					opno,
					opcode;

		entry = &result[i];
		relattno = ident_idx->indkey.values[i];
		if (relattno >= 1)
		{
			TupleDesc	desc;

			desc = rel_src->rd_att;
			att = TupleDescAttr(desc, relattno - 1);
		}
		else
			elog(ERROR, "Unexpected attribute number %d in index", relattno);

		opfamily = ident_idx_rel->rd_opfamily[i];
		opcintype = ident_idx_rel->rd_opcintype[i];
		opno = get_opfamily_member(opfamily, opcintype, opcintype,
								   BTEqualStrategyNumber);

		if (!OidIsValid(opno))
			elog(ERROR, "Failed to find = operator for type %u", opcintype);

		opcode = get_opcode(opno);
		if (!OidIsValid(opcode))
			elog(ERROR, "Failed to find = operator for operator %u", opno);

		/* Initialize everything but argument. */
		ScanKeyInit(entry,
					i + 1,
					BTEqualStrategyNumber, opcode,
					(Datum) NULL);
		entry->sk_collation = att->attcollation;
	}
	index_close(ident_idx_rel, AccessShareLock);

	*nentries = n;
	return result;
}

/*
 * Try to perform the final processing of concurrent data changes of the
 * source table, which requires an exclusive lock. The return value tells
 * whether this step succeeded. (If not, caller might want to retry.)
 */
static bool
perform_final_merge(Oid relid_src, Oid *indexes_src, int nindexes,
					Relation rel_dst, ScanKey ident_key,
					int ident_key_nentries, IndexInsertState *iistate,
					CatalogState *cat_state,
					LogicalDecodingContext *ctx)
{
	bool		success;
	XLogRecPtr	xlog_insert_ptr,
				end_of_wal;
	int			i;
	struct timeval t_end;
	struct timeval *t_end_ptr = NULL;
	char		dummy_rec_data = '\0';

	/*
	 * Lock the source table exclusively last time, to finalize the work.
	 *
	 * On pg_repack: before taking the exclusive lock, pg_repack extension is
	 * more restrictive in waiting for other transactions to complete. That
	 * might reduce the likelihood of MVCC-unsafe behavior that PG core admits
	 * in some cases
	 * (https://www.postgresql.org/docs/9.6/static/mvcc-caveats.html) but
	 * can't completely avoid it anyway. On the other hand, pg_squeeze only
	 * waits for completion of transactions which performed write (i.e. do
	 * have XID assigned) - this is a side effect of bringing our replication
	 * slot into consistent state.
	 *
	 * As pg_repack shows, extra effort makes little sense here, because some
	 * other transactions still can start before the exclusive lock on the
	 * source relation is acquired. In particular, if transaction A starts in
	 * this period and commits a change, transaction B can miss it if the next
	 * steps are as follows: 1. transaction B took a snapshot (e.g. it has
	 * REPEATABLE READ isolation level), 2. pg_repack took the exclusive
	 * relation lock and finished its work, 3. transaction B acquired shared
	 * lock and performed its scan. (And of course, waiting for transactions
	 * A, B, ... to complete while holding the exclusive lock can cause
	 * deadlocks.)
	 */
	LockRelationOid(relid_src, AccessExclusiveLock);

	/*
	 * Lock the indexes too, as ALTER INDEX does not need table lock.
	 *
	 * The locking will succeed even if the index is no longer there. In that
	 * case, ERROR will be raised during the catalog check below.
	 */
	for (i = 0; i < nindexes; i++)
		LockRelationOid(indexes_src[i], AccessExclusiveLock);

	if (squeeze_max_xlock_time > 0)
	{
		int64		usec;
		struct timeval t_start;

		gettimeofday(&t_start, NULL);
		/* Add the whole seconds. */
		t_end.tv_sec = t_start.tv_sec + squeeze_max_xlock_time / 1000;
		/* Add the rest, expressed in microseconds. */
		usec = t_start.tv_usec + 1000 * (squeeze_max_xlock_time % 1000);
		/* The number of microseconds could have overflown. */
		t_end.tv_sec += usec / USECS_PER_SEC;
		t_end.tv_usec = usec % USECS_PER_SEC;
		t_end_ptr = &t_end;

		elog(DEBUG1,
			 "pg_squeeze: completion required by %lu.%lu, current time is %lu.%lu.",
			 t_end_ptr->tv_sec, t_end_ptr->tv_usec, t_start.tv_sec,
			 t_start.tv_usec);

	}

	/*
	 * Check the source relation for DDLs once again. If this check passes, no
	 * DDL can break the process anymore. NoLock must be passed because the
	 * relation was really unlocked for some period since the last check.
	 *
	 * It makes sense to do this immediately after having acquired the
	 * exclusive lock(s), so we don't waste any effort if the source table is
	 * no longer compatible.
	 */
	check_catalog_changes(cat_state, NoLock);

	/*
	 * Flush anything we see in WAL, to make sure that all changes committed
	 * while we were creating indexes and waiting for the exclusive lock are
	 * available for decoding. This should not be necessary if all backends
	 * had synchronous_commit set, but we can't rely on this setting.
	 *
	 * Unfortunately, GetInsertRecPtr() may lag behind the actual insert
	 * position, and GetLastImportantRecPtr() points at the start of the last
	 * record rather than at the end. Thus the simplest way to determine the
	 * insert position is to insert a dummy record and use its LSN.
	 *
	 * XXX Consider using GetLastImportantRecPtr() and adding the size of the
	 * last record (plus the total size of all the page headers the record
	 * spans)?
	 */
	XLogBeginInsert();
	XLogRegisterData(&dummy_rec_data, 1);
	xlog_insert_ptr = XLogInsert(RM_XLOG_ID, XLOG_NOOP);
	XLogFlush(xlog_insert_ptr);
#if PG_VERSION_NUM >= 150000
	end_of_wal = GetFlushRecPtr(NULL);
#else
	end_of_wal = GetFlushRecPtr();
#endif

	/*
	 * Process the changes that might have taken place while we were waiting
	 * for the lock.
	 *
	 * AccessExclusiveLock effectively disables catalog checks - we've already
	 * performed them above.
	 */
	success = process_concurrent_changes(ctx, end_of_wal,
										 cat_state, rel_dst, ident_key,
										 ident_key_nentries, iistate,
										 AccessExclusiveLock, t_end_ptr);
	if (t_end_ptr)
	{
		struct timeval t_now;

		gettimeofday(&t_now, NULL);
		elog(DEBUG1,
			 "pg_squeeze: concurrent changes processed at %lu.%lu, result: %u",
			 t_now.tv_sec, t_now.tv_usec, success);
	}

	if (!success)
	{
		/* Unlock the relations and indexes. */
		for (i = 0; i < nindexes; i++)
			UnlockRelationOid(indexes_src[i], AccessExclusiveLock);

		UnlockRelationOid(relid_src, AccessExclusiveLock);

		/*
		 * Take time to reach end_of_wal.
		 *
		 * XXX DecodingOutputState may contain some changes. The corner case
		 * that the data_size has already reached maintenance_work_mem so the
		 * first change we decode now will make it spill to disk is too low to
		 * justify calling apply_concurrent_changes() separately.
		 */
		process_concurrent_changes(ctx, end_of_wal,
								   cat_state, rel_dst, ident_key,
								   ident_key_nentries, iistate,
								   AccessExclusiveLock, NULL);

		/* No time constraint, all changes must have been processed. */
		Assert(((DecodingOutputState *)
				ctx->output_writer_private)->nchanges == 0);
	}

	return success;
}

/*
 * Derived from swap_relation_files() in PG core, but removed anything we
 * don't need. Also incorporated the relevant parts of finish_heap_swap().
 *
 * Caution: r1 is the relation to remain, r2 is the one to be dropped.
 *
 * XXX Unlike PG core, we currently receive neither frozenXid nor cutoffMulti
 * arguments. Instead we only copy these fields from r2 to r1. This should
 * change if we preform regular rewrite instead of INSERT INTO ... SELECT ...
 */
static void
swap_relation_files(Oid r1, Oid r2)
{
	Relation	relRelation;
	HeapTuple	reltup1,
				reltup2;
	Form_pg_class relform1,
				relform2;
	Oid			relfilenode1,
				relfilenode2;
	Oid			swaptemp;
	CatalogIndexState indstate;

	/* We need writable copies of both pg_class tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	relfilenode1 = relform1->relfilenode;
	relfilenode2 = relform2->relfilenode;

	if (OidIsValid(relfilenode1) && OidIsValid(relfilenode2))
	{
		swaptemp = relform1->relfilenode;
		relform1->relfilenode = relform2->relfilenode;
		relform2->relfilenode = swaptemp;

		swaptemp = relform1->reltablespace;
		relform1->reltablespace = relform2->reltablespace;
		relform2->reltablespace = swaptemp;

		Assert(relform1->relpersistence == relform2->relpersistence);
		Assert(relform1->relam == relform2->relam);

		swaptemp = relform1->reltoastrelid;
		relform1->reltoastrelid = relform2->reltoastrelid;
		relform2->reltoastrelid = swaptemp;
	}
	else
		elog(ERROR, "cannot swap mapped relations");

	/*
	 * Set rel1's frozen Xid and minimum MultiXid.
	 */
	if (relform1->relkind != RELKIND_INDEX)
	{
		TransactionId frozenXid;
		MultiXactId cutoffMulti;

		frozenXid = RecentXmin;
		Assert(TransactionIdIsNormal(frozenXid));

		/*
		 * Unlike CLUSTER command (see copy_heap_data()), we don't derive the
		 * new value from any freeze-related configuration parameters, so
		 * there should be no way to see the value go backwards.
		 */
		Assert(!TransactionIdPrecedes(frozenXid, relform2->relfrozenxid));
		relform1->relfrozenxid = frozenXid;

		cutoffMulti = GetOldestMultiXactId();
		Assert(MultiXactIdIsValid(cutoffMulti));
		Assert(!MultiXactIdPrecedes(cutoffMulti, relform2->relminmxid));
		relform1->relminmxid = cutoffMulti;
	}

	/*
	 * Adjust pg_class fields of the relation (relform2 can be ignored as the
	 * transient relation will get dropped.)
	 *
	 * There's no reason to expect relallvisible to be non-zero. The next
	 * VACUUM should fix it.
	 *
	 * As for relpages and reltuples, neither includes concurrent changes (are
	 * those worth any calculation?), so leave the original values. The next
	 * ANALYZE will fix them.
	 */
	relform1->relallvisible = 0;

	indstate = CatalogOpenIndexes(relRelation);
	CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1,
							   indstate);
	CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2,
							   indstate);
	CatalogCloseIndexes(indstate);

	InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0,
								 InvalidOid, true);
	InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0,
								 InvalidOid, true);

	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		ObjectAddress baseobject,
					toastobject;
		long		count;

		if (IsSystemClass(r1, relform1))
			elog(ERROR, "cannot swap toast files by links for system catalogs");

		if (relform1->reltoastrelid)
		{
			count = deleteDependencyRecordsFor(RelationRelationId,
											   relform1->reltoastrelid,
											   false);
			if (count != 1)
				elog(ERROR, "expected one dependency record for TOAST table, found %ld",
					 count);
		}
		if (relform2->reltoastrelid)
		{
			count = deleteDependencyRecordsFor(RelationRelationId,
											   relform2->reltoastrelid,
											   false);
			if (count != 1)
				elog(ERROR, "expected one dependency record for TOAST table, found %ld",
					 count);
		}

		baseobject.classId = RelationRelationId;
		baseobject.objectSubId = 0;
		toastobject.classId = RelationRelationId;
		toastobject.objectSubId = 0;

		if (relform1->reltoastrelid)
		{
			baseobject.objectId = r1;
			toastobject.objectId = relform1->reltoastrelid;
			recordDependencyOn(&toastobject, &baseobject,
							   DEPENDENCY_INTERNAL);
		}

		if (relform2->reltoastrelid)
		{
			baseobject.objectId = r2;
			toastobject.objectId = relform2->reltoastrelid;
			recordDependencyOn(&toastobject, &baseobject,
							   DEPENDENCY_INTERNAL);
		}
	}

	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	table_close(relRelation, RowExclusiveLock);

#if PG_VERSION_NUM < 170000
	RelationCloseSmgrByOid(r1);
	RelationCloseSmgrByOid(r2);
#endif
}

/*
 * Swap TOAST relation names if needed.
 *
 * relid1 represents the relation to stay and toastrelid1 its TOAST relation.
 * relid2 refer to the transient relation in the same manner.
 *
 * The storage of TOAST tables and their indexes have already been swapped.
 *
 * On exit we hold AccessExclusiveLock on the TOAST relations and their indexes.
 */
static void
swap_toast_names(Oid relid1, Oid toastrelid1, Oid relid2, Oid toastrelid2)
{
	char		name[NAMEDATALEN];
	Oid			toastidxid;

	/*
	 * If relid1 no longer needs TOAST, we don't even rename that of relid2.
	 */
	if (!OidIsValid(toastrelid1))
		return;

	if (OidIsValid(toastrelid2))
	{
		/*
		 * Added underscore should be enough to keep names unique (at least
		 * within the pg_toast namespace). This assumption makes name
		 * retrieval unnecessary.
		 */
		snprintf(name, NAMEDATALEN, "pg_toast_%u_", relid1);
		RenameRelationInternal(toastrelid2, name, true, false);

		snprintf(name, NAMEDATALEN, "pg_toast_%u_index_", relid1);
#if PG_VERSION_NUM < 130000
		/* NoLock as RenameRelationInternal() did not release its lock. */
		toastidxid = get_toast_index(toastrelid2);
#else
		/* TOAST relation is locked, but not its indexes. */
		toastidxid = toast_get_valid_index(toastrelid2, AccessExclusiveLock);
#endif
		/*
		 * Pass is_index=false so that even the index is locked in
		 * AccessExclusiveLock mode. ShareUpdateExclusiveLock mode (allowing
		 * concurrent read / write access to the index or even its renaming)
		 * should not be a problem at this stage of table squeezing, but it'd
		 * also bring little benefit (the table is locked exclusively, so no
		 * one should need read / write access to the TOAST indexes).
		 */
		RenameRelationInternal(toastidxid, name, true, false);
		CommandCounterIncrement();
	}

	/* Now set the desired names on the TOAST stuff of relid1. */
	snprintf(name, NAMEDATALEN, "pg_toast_%u", relid1);
	RenameRelationInternal(toastrelid1, name, true, false);
#if PG_VERSION_NUM < 130000
	/* NoLock as RenameRelationInternal() did not release its lock. */
	toastidxid = get_toast_index(toastrelid1);
#else
	/* TOAST relation is locked, but not its indexes. */
	toastidxid = toast_get_valid_index(toastrelid1, AccessExclusiveLock);
#endif
	snprintf(name, NAMEDATALEN, "pg_toast_%u_index", relid1);
	RenameRelationInternal(toastidxid, name, true, false);
	CommandCounterIncrement();
}

#if PG_VERSION_NUM < 130000
/*
 * The function is called after RenameRelationInternal() which does not
 * release the lock it acquired.
 */
static Oid
get_toast_index(Oid toastrelid)
{
	Relation	toastrel;
	List	   *toastidxs;
	Oid			result;

	toastrel = table_open(toastrelid, NoLock);
	toastidxs = RelationGetIndexList(toastrel);

	if (toastidxs == NIL || list_length(toastidxs) != 1)
		elog(ERROR, "Unexpected number of TOAST indexes");

	result = linitial_oid(toastidxs);
	table_close(toastrel, NoLock);

	return result;
}
#endif

/*
 * Retrieve the "fillfactor" storage option in a convenient way, so we don't
 * have to parse pg_class(reloptions) value at SQL level.
 */
extern Datum get_heap_fillfactor(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_heap_fillfactor);
Datum
get_heap_fillfactor(PG_FUNCTION_ARGS)
{
	Oid			relid;
	Relation	rel;
	int			fillfactor;

	relid = PG_GETARG_OID(0);

	/*
	 * XXX Not sure we need stronger lock - there are still occasions for
	 * others to change the fillfactor (or even drop the relation) after this
	 * function has returned.
	 */
	rel = table_open(relid, AccessShareLock);
	fillfactor = RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR);
	table_close(rel, AccessShareLock);
	PG_RETURN_INT32(fillfactor);
}

/*
 * Return fraction of free space in a relation, as indicated by FSM.
 */
extern Datum get_heap_freespace(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_heap_freespace);
Datum
get_heap_freespace(PG_FUNCTION_ARGS)
{
	Oid			relid;
	Relation	rel;
	BlockNumber blkno,
				nblocks;
	Size		free,
				total;
	float8		result;
	bool		fsm_exists = true;

	relid = PG_GETARG_OID(0);
	rel = table_open(relid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(rel);

	/* NULL makes more sense than zero free space. */
	if (nblocks == 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	free = 0;
	total = 0;
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		free += GetRecordedFreeSpace(rel, blkno);
		total += BLCKSZ;
	}

	/*
	 * If the relation seems to be full, verify that missing FSM is not the
	 * reason.
	 */
	if (free == 0)
	{
#if PG_VERSION_NUM >= 150000
		if (!smgrexists(RelationGetSmgr(rel), FSM_FORKNUM))
			fsm_exists = false;
#else
		RelationOpenSmgr(rel);
		if (!smgrexists(rel->rd_smgr, FSM_FORKNUM))
			fsm_exists = false;
#endif
		RelationCloseSmgr(rel);
	}
	table_close(rel, AccessShareLock);

	if (!fsm_exists)
		PG_RETURN_NULL();

	result = (float8) free / total;
	PG_RETURN_FLOAT8(result);
}

/*
 * Handle an error from the perspective of postgres
 */
void
squeeze_handle_error_db(ErrorData **edata_p, MemoryContext edata_cxt)
{
	MemoryContext old_context = CurrentMemoryContext;

	HOLD_INTERRUPTS();

	/* Save error info in caller's context */
	MemoryContextSwitchTo(edata_cxt);
	*edata_p = CopyErrorData();
	MemoryContextSwitchTo(old_context);

	/*
	 * Send the message to the process that assigned the task.
	 */
	strlcpy(MyWorkerTask->error_msg, (*edata_p)->message,
			ERROR_MESSAGE_MAX_SIZE);

	/*
	 * Abort the transaction as we do not call PG_RE_THROW() below in this
	 * case.
	 */
	if (IsTransactionState())
		AbortOutOfAnyTransaction();

	/*
	 * Now that the transaction is aborted, we can run a new one to drop the
	 * origin.
	 */
	if (replorigin_session_origin != InvalidRepOriginId)
		manage_session_origin(InvalidOid);

	/*
	 * Special effort is needed to release the replication slot because,
	 * unlike other resources, AbortTransaction() does not release it.
	 */
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	RESUME_INTERRUPTS();
}

/*
 * If 'relid' is valid, create replication origin and set the
 * replorigin_session_origin variable. If 'relid' is InvalidOid, drop the
 * origin created earlier and clear replorigin_session_origin.
 *
 * (The origin is used here to mark WAL records produced by the extension,
 * rather than for real replication.)
 */
void
manage_session_origin(Oid relid)
{
	static Oid my_relid = InvalidOid;
	char	origin_name[NAMEDATALEN];
	Oid             origin;

	snprintf(origin_name, sizeof(origin_name),
			 REPLORIGIN_NAME_PATTERN, MyDatabaseId,
			 OidIsValid(relid) ? relid : my_relid);

	StartTransactionCommand();
	if (OidIsValid(relid))
	{
		origin = replorigin_create(origin_name);
		/*
		 * As long as we set replorigin_session_origin below, we should setup
		 * the session state because both RecordTransactionCommit() and
		 * RecordTransactionAbort() do expect that.
		 */
#if PG_VERSION_NUM >= 160000
		replorigin_session_setup(origin, 0);
#else
		replorigin_session_setup(origin);
#endif
		Assert(replorigin_session_origin == InvalidRepOriginId);
		replorigin_session_origin = origin;

		Assert(!OidIsValid(my_relid));
		my_relid = relid;
	}
	else
	{
		replorigin_session_reset();

#if PG_VERSION_NUM >= 140000
		replorigin_drop_by_name(origin_name, false, true);
#else
		replorigin_drop(replorigin_session_origin, false);
#endif
		replorigin_session_origin = InvalidRepOriginId;

		Assert(OidIsValid(my_relid));
		my_relid = InvalidOid;
	}
	CommitTransactionCommand();
}
