#include "pg_squeeze.h"

#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_am.h"
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
#include "optimizer/planner.h"
#include "replication/logicalfuncs.h"
#include "replication/snapbuild.h"
#include "storage/lmgr.h"
#include "storage/standbydefs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

PG_MODULE_MAGIC;

#define	REPL_SLOT_NAME		"pg_squeeze_slot"
#define	REPL_PLUGIN_NAME	"pg_squeeze"

static int index_cat_info_compare(const void *arg1, const void *arg2);

/* Index-to-tablespace mapping. */
typedef struct IndexTablespace
{
	Oid	index;
	Oid	tablespace;
} IndexTablespace;

/* Where should the new table and its indexes be located? */
typedef struct TablespaceInfo
{
	Oid	table;

	int	nindexes;
	IndexTablespace *indexes;
} TablespaceInfo;

static void check_prerequisites(Relation rel);
static LogicalDecodingContext *setup_decoding(Oid relid, TupleDesc tup_desc);
static void decoding_cleanup(LogicalDecodingContext *ctx);
static CatalogState *get_catalog_state(Oid relid);
static TransactionId *get_attribute_xmins(Oid relid, int relnatts,
										  Snapshot snapshot);
static IndexCatInfo *get_index_info(Oid relid, int *relninds,
									bool *found_invalid, bool invalid_check_only,
									Snapshot snapshot);
static void check_attribute_changes(Oid relid, TransactionId	*attrs, int relnatts);
static void check_index_changes(Oid relid, IndexCatInfo *indexes,
								int relninds);
static void free_catalog_state(CatalogState *state);
static void check_pg_class_changes(Oid relid, TransactionId xmin,
								   LOCKMODE lock_held);
static void free_tablespace_info(TablespaceInfo *tbsp_info);
static void resolve_index_tablepaces(TablespaceInfo *tbsp_info,
									 CatalogState *cat_state,
									 ArrayType *ind_tbsp_a);
static void switch_snapshot(Snapshot snap_hist);
static void perform_initial_load(Relation rel_src, RangeVar *cluster_idx_rv,
								 Snapshot snap_hist, Relation rel_dst);
static Oid create_transient_table(CatalogState *cat_state, TupleDesc tup_desc,
								  Oid tablespace, Oid relowner);
static Oid *build_transient_indexes(Relation rel_dst, Relation rel_src,
									Oid *indexes_src, int nindexes,
									TablespaceInfo *tbsp_info,
									CatalogState *cat_state);
static ScanKey build_identity_key(Oid ident_idx_oid, Relation rel_src,
								  int *nentries);
static bool perform_final_merge(Oid relid_src, Oid *indexes_src, int nindexes,
								Relation rel_dst, ScanKey ident_key,
								int ident_key_nentries,
								IndexInsertState *iistate,
								CatalogState *cat_state,
								LogicalDecodingContext *ctx,
								XLogRecPtr	*startptr);
static void swap_relation_files(Oid r1, Oid r2);
static void swap_toast_names(Oid relid1, Oid toastrelid1, Oid relid2,
							 Oid toastrelid2);
static Oid get_toast_index(Oid toastrelid);
static void update_reloptions(Oid relid, bool user_cat, bool autovac);

int squeeze_worker_naptime;

/*
 * The maximum time to hold AccessExclusiveLock during the final
 * processing. Note that it only process_concurrent_changes() execution time
 * is included here. The very last steps like swap_relation_files() and
 * swap_toast_names() shouldn't get blocked and it'd be wrong to consider them
 * a reason to abort otherwise completed processing.
 */
int squeeze_max_xlock_time = 0;

/*
 * List of database OIDs for which the background worker should start started
 * during cluster startup. (We require OIDs because there seems to be now good
 * way to pass list of database name w/o adding restrictions on character set
 * characters.)
 */
char *squeeze_worker_autostart = NULL;

/*
 * OID of the role on behalf of which automatically-started worker connects to
 * database(s).
 */
int squeeze_worker_role = 0;

void
_PG_init(void)
{
	/*
	 * PG core does not support "user_catalog_table" option for TOAST, so add
	 * it as a custom option.
	 */
	add_bool_reloption(RELOPT_KIND_TOAST, "user_catalog_table", "", false);

	DefineCustomStringVariable(
		"squeeze.worker_autostart",
		"OIDs of databases for which squeeze worker starts automatically.",
		"Comma-separated list for which squeeze worker starts as soon as "
		"the cluster startup has completed.",
		&squeeze_worker_autostart,
		false,
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"squeeze.worker_role",
		"OID of role that background worker uses to connect to database.",
		"If background worker was launched automatically on cluster startup, "
		"it uses this role to initiate database connection(s).",
		&squeeze_worker_role,
		/*
		 * 1 would be more convenient as the minimum, but we need to preserve
		 * the value 0 as boot value - that indicates that the parameter is
		 * not set at all. (User has no reason to set 0 explicitly.)
		 */
		0, 0, INT_MAX,
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"squeeze.worker_naptime",
		"Sleep time (in seconds) of the squeeze worker.",
		"If there are no tables eligible for squeezing, the background worker "
		"sleeps this amount of seconds and then tries again.",
		&squeeze_worker_naptime,
		60, 1, INT_MAX,
		PGC_SIGHUP,
		GUC_UNIT_S,
		NULL, NULL, NULL);

	if (squeeze_worker_autostart)
	{
		BackgroundWorker worker;
		Oid	role;
		Datum	d;
		oidvector	*oidvec;
		int	i;

		if (squeeze_worker_role == 0)
			ereport(ERROR,
					(errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING),
					 (errmsg("\"squeeze.worker_role\" is invalid or not set"))));

		role = (Oid) squeeze_worker_role;
		d = DirectFunctionCall1(oidvectorin,
								CStringGetDatum(squeeze_worker_autostart));

		/*
		 * oidvector is stored as varlena, use DatumGetByteaP() for
		 * convenience.
		 */
		oidvec = (oidvector *) DatumGetByteaP(d);

		/* Register the workers one after another. */
		for (i = 0; i < oidvec->dim1; i++)
		{
			squeeze_initialize_bgworker(&worker, squeeze_worker_main,
										oidvec->values[i], role, 0);
			RegisterBackgroundWorker(&worker);
		}
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
 * SQL interface to squeeze one table interactively.
 *
 * "user_catalog_table" option must have been set by a separate transaction
 * (otherwise the replication slot causes ERROR instead of infinite waiting
 * for consistent state). We try to clear it as soon as possible. Caller
 * should check (and possibly clear) the option if the function failed in any
 * way.
 */
extern Datum squeeze_table(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(squeeze_table);
Datum
squeeze_table(PG_FUNCTION_ARGS)
{
	Name	   relschema, relname;
	RangeVar   *relrv_src;
	RangeVar	*relrv_cl_idx = NULL;
	Relation	rel_src, rel_dst;
	Oid	rel_src_owner;
	Oid	ident_idx_src, ident_idx_dst;
	Oid	relid_src, relid_dst;
	Oid	toastrelid_src, toastrelid_dst;
	List	*relsrc_indlist;
	char	replident;
	ScanKey	ident_key;
	int	i, ident_key_nentries;
	IndexInsertState	*iistate;
	LogicalDecodingContext	*ctx;
	Snapshot	snap_hist;
	TupleDesc	tup_desc;
	CatalogState		*cat_state;
	XLogRecPtr	end_of_wal, startptr;
	XLogRecPtr	xlog_insert_ptr;
	int	nindexes;
	Oid	*indexes_src = NULL, *indexes_dst = NULL;
	bool	invalid_index = false;
	IndexCatInfo	*ind_info;
	TablespaceInfo	*tbsp_info;
	ObjectAddress	object;
	bool	autovac;
	bool	source_finalized;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 (errmsg("Both schema and table name must be specified"))));

	/*
	 * These 2 are located later in the argument list, but the check is too
	 * simple to be postponed.
	 *
	 * "autovac_toast" is enforced to be NOT NULL even though we don't know if
	 * the table has TOAST. It'd be strange to raise ERROR due to NULL
	 * argument much later, when we can reliably check the existence of TOAST.
	 */
	if (PG_ARGISNULL(5) || PG_ARGISNULL(6))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 (errmsg("Both \"autovac\" and \"autovac_toast\" arguments must be specified"))));

	relschema = PG_GETARG_NAME(0);
	relname = PG_GETARG_NAME(1);
	relrv_src = makeRangeVar(NameStr(*relschema), NameStr(*relname), -1);
	rel_src = heap_openrv(relrv_src, AccessShareLock);

	check_prerequisites(rel_src);

	/*
	 * Refresh the index list.
	 *
	 * Although our processing of the list shouldn't cause invalidation of
	 * this particular relcache entry (RelationGetIndexList() comment mentions
	 * invalidation due to cache lookup, but we'll only lookup tablespaces and
	 * index relations) it seems at least cleaner to use the returned copy, as
	 * opposed to rel_src->rd_indexlist. Thus we can eventually free it
	 * explicitly.
	 */
	relsrc_indlist = RelationGetIndexList(rel_src);

	/*
	 * Retrieve other useful info while holding lock on the relation.
	 */
	replident = rel_src->rd_rel->relreplident;
	ident_idx_src = RelationGetReplicaIndex(rel_src);
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
	 * The "user_catalog_table" relation option is essential for the initial
	 * scan, otherwise some useful tuples might get VACUUMed before we
	 * retrieve them. It must be enabled before
	 * ReplicationSlotsComputeRequiredXmin() sets up new catalog_xmin so that
	 * GetOldestXmin() treats the table like catalog one as soon as it sees
	 * the new catalog_min published.
	 *
	 * XXX It'd still be correct to start the check a bit later, i.e. just
	 * before CreateInitDecodingContext(), but the gain is not worth making
	 * the code less readable.
	 */
	cat_state = get_catalog_state(relid_src);

	/* Give up if it's clear enough to do. */
	if (!cat_state->is_catalog)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("Table or its TOAST table do not have \"user_catalog_table\" option set"))));
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
	 */
	heap_close(rel_src, AccessShareLock);

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
	 * this error code seems to be the best
	 * match. (ERRCODE_TRIGGERED_ACTION_EXCEPTION might be worth consideration
	 * as well.)
	 */
	if (replident == REPLICA_IDENTITY_NOTHING ||
		(replident == REPLICA_IDENTITY_DEFAULT && !OidIsValid(ident_idx_src)))
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Table \"%s\".\"%s\" has no identity index",
						 NameStr(*relschema), NameStr(*relname)))));

	/* Change processing w/o index is not a good idea. */
	if (replident == REPLICA_IDENTITY_FULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Replica identity \"full\" not supported"))));

	/*
	 * Clustering index, if any.
	 *
	 * Do not lock the index so far, e.g. just to retrieve OID and to keep it
	 * valid. Neither the relation can be locked continuously, so by keeping
	 * the index locked alone we'd introduce incorrect order of
	 * locking. Although we use only share locks in most cases (so I'm not
	 * aware of particular deadlock scenario), it doesn't seem wise. The worst
	 * consequence of not locking is that perform_initial_load() will error
	 * out.
	 */
	if (!PG_ARGISNULL(2))
	{
		Name	indname;

		indname = PG_GETARG_NAME(2);
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
	if (!PG_ARGISNULL(3))
	{
		Name	tbspname;


		tbspname = PG_GETARG_NAME(3);
		tbsp_info->table = get_tablespace_oid(pstrdup(NameStr(*tbspname)),
											  false);
	}
	else
		tbsp_info->table = cat_state->form_class->reltablespace;

	/* Index-to-tablespace mappings. */
	if (!PG_ARGISNULL(4))
	{
		ArrayType	*ind_tbsp = PG_GETARG_ARRAYTYPE_P(4);

		resolve_index_tablepaces(tbsp_info, cat_state, ind_tbsp);
	}
	list_free(relsrc_indlist);

	/*
	 * TODO If the number of columns is non-zero, check if there's at least
	 * one valid (not dropped).
	 */
	if (cat_state->form_class->relnatts < 1)
		/* XXX Try to find better error code. */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 (errmsg("Table \"%s\" has no valid columns",
						 relrv_src->relname))));

	/* Therefore the total number of indexes is non-zero. */
	nindexes = cat_state->relninds;
	Assert(nindexes > 0);

	/* Copy the OIDs into a separate array, for convenient use later. */
	indexes_src = (Oid *) palloc(nindexes * sizeof(Oid));
	for (i = 0; i < nindexes; i++)
		indexes_src[i] = cat_state->indexes[i].oid;

	ctx = setup_decoding(relid_src, tup_desc);

	/*
	 * The first consistent snapshot should be available now. There's no
	 * reason to look for another snapshot for the "main query".
	 *
	 * XXX The 2nd argument is currently unused in PG core.
	 */
	snap_hist = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder,
											InvalidTransactionId);

	relid_dst = create_transient_table(cat_state, tup_desc, tbsp_info->table,
		rel_src_owner);

	/* The source relation will be needed for the initial load. */
	rel_src = heap_open(relid_src, AccessShareLock);

	/* NoLock, it's only visible by us. */
	rel_dst = heap_open(relid_dst, NoLock);

	toastrelid_dst = rel_dst->rd_rel->reltoastrelid;

	/*
	 * We need at least to know whether the catalog option was never changed,
	 * and that no DDL took place that allows for data inconsistency. That
	 * includes removal of the "user_catalog_table" option.
	 *
	 * The relation was unlocked for some time since last check, so pass
	 * NoLock.
	 */
	check_catalog_changes(cat_state, NoLock);

	/*
	 * The historic snapshot is used to retrieve data w/o concurrent
	 * changes. It must not stay active afterwards.
	 */
	PG_TRY();
	{
		perform_initial_load(rel_src, relrv_cl_idx, snap_hist, rel_dst);
		switch_snapshot(NULL);
	}
	PG_CATCH();
	{
		switch_snapshot(NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Check for concurrent changes that would make us stop working later.
	 * Index build can take quite some effort and we don't want to waste it.
	 *
	 * Note: By still holding the share lock we only ensure that the source
	 * relation is not altered underneath index build, but we'll have to
	 * release the lock for a short time at some point. So while we can't
	 * prevent anyone from forcing us to cancel our work,such cancellation
	 * must happen at well-defined moment.
	 */
	check_catalog_changes(cat_state, AccessShareLock);

	/*
	 * Make sure the contents of the transient table contents if visible for
	 * the scan(s) during index build.
	 */
	CommandCounterIncrement();

	/*
	 * Create indexes on the temporary table - that might take a
	 * while. (Unlike the concurrent changes, which we insert into existing
	 * indexes.)
	 */
	PushActiveSnapshot(GetTransactionSnapshot());
	indexes_dst = build_transient_indexes(rel_dst, rel_src, indexes_src,
										  nindexes, tbsp_info, cat_state);
	PopActiveSnapshot();

	/*
	 * Make the identity index of the transient table visible, for the sake of
	 * concurrent UPDATEs and DELETEs.
	 */
	CommandCounterIncrement();

	/* Tablespace info is no longer needed. */
	free_tablespace_info(tbsp_info);

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
	heap_close(rel_src, AccessShareLock);

	/*
	 * Valid identity index should exist now, see the identity checks above.
	 */
	Assert(OidIsValid(ident_idx_src));

	/*
	 * Build scan key that we'll use to look for rows to be updated / deleted
	 * during logical decoding.
	 */
	ident_key = build_identity_key(ident_idx_src, rel_src,
								   &ident_key_nentries);

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
	end_of_wal = GetFlushRecPtr();
	startptr = MyReplicationSlot->data.restart_lsn;

	/*
	 * Decode and apply the data changes that occurred while the initial load
	 * was in progress.
	 *
	 * Even if the amount of concurrent changes of our source table might not
	 * be significant, both initial load and index build could have produced
	 * many XLOG records that we need to read. Do so before requesting
	 * exclusive lock on the source relation.
	 */
	process_concurrent_changes(ctx, &startptr, end_of_wal, cat_state, rel_dst,
							   ident_key, ident_key_nentries, iistate,
							   NoLock, NULL);

	/*
	 * This (supposedly cheap) special check should avoid one particular
	 * deadlock scenario: another transaction, performing index DDL
	 * concurrenly (e.g. DROP INDEX CONCURRENTLY) committed change of
	 * indisvalid, indisready, ... and called WaitForLockers() before we
	 * unlocked both source table and its indexes above. Since our transaction
	 * is still running, the other transaction keeps waiting, but holds
	 * (non-exclusive) lock on both relation and index. In this situation we'd
	 * cause deadlock by requesting exclusive lock. Fortunately we should
	 * recognize this scenario by checking pg_index.
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
								iistate, cat_state, ctx, &startptr))
		{
			source_finalized = true;
			break;
		}
		else
			elog(DEBUG1,
				 "Exclusive lock on table %u had to be released.", relid_src);
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
	/* XXX (Should have been closed right after
	 * process_concurrent_changes()?) */
	heap_close(rel_dst, NoLock);

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

	/*
	 * The "user_catalog_table" option can be cleared now, but we don't want
	 * to commit the current transaction. (VACUUM workers won't notice this
	 * change until our commit, so they might see just limited amount of
	 * tuples eligible for processing. That's o.k., as the new relation will
	 * be visible after commit and that shouldn't contain VACUUMable tuples
	 * anyway.)
	 */
	autovac = PG_GETARG_BOOL(5);
	update_reloptions(cat_state->relid, false, autovac);

	if (OidIsValid(toastrelid_dst))
	{
		bool	autovac_toast;

		/*
		 * toastrelid_dst points to the TOAST relation that remains. We've
		 * copied the reloptions from the original TOAST relation. Now we need
		 * to clear the "user_catalog_table" option and also set the
		 * "autovacuum_enabled" option as required by
		 * caller. (swap_toast_names() provides us with AccessExclusiveLock on
		 * the relation, but the table shouldn't be visible to other backends
		 * yet.)
		 */
		autovac_toast = PG_GETARG_BOOL(6);
		update_reloptions(toastrelid_dst, false, autovac_toast);
	}

	/* State not needed anymore. */
	free_catalog_state(cat_state);

	/*
	 * Drop the transient table including indexes (constraints would be
	 * dropped this way too, but we haven't created any).
	 */
	object.classId = RelationRelationId;
	object.objectSubId = 0;
	object.objectId = relid_dst;
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	PG_RETURN_VOID();
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
	Form_pg_class	form = RelationGetForm(rel);

	/* Check the relation first. */
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

	if (RelationIsMapped(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is mapped relation",
						RelationGetRelationName(rel))));

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
 * This function is much like pg_create_logical_replication_slot() except that
 * the new slot is neither released (if anyone else could read changes from
 * our slot, we could miss changes other backends do while we copy the
 * existing data into temporary table), nor persisted (it's easier to handle
 * crash by restarting all the work from scratch).
 *
 * XXX Even though CreateInitDecodingContext() does not set state to
 * RS_PERSISTENT, it does write the slot to disk. We rely on
 * RestoreSlotFromDisk() to delete ephemeral slots during startup. (Both ERROR
 * and FATAL should lead to cleanup even before the cluster goes down.)
 */
static LogicalDecodingContext *
setup_decoding(Oid relid, TupleDesc tup_desc)
{
	LogicalDecodingContext *ctx;
	DecodingOutputState	*dstate;
	MemoryContext oldcontext;

	/*
	 * postgres.c should have done the cleanup if the squeeze_table() was
	 * called interactively, but that does not happen if it was called from
	 * plpgsql function and the ERROR was trapped.
	 */
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	/* check_permissions() "inlined", as logicalfuncs.c does not export it.*/
	if (!superuser() && !has_rolreplication(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser or replication role to use replication slots"))));

	CheckLogicalDecodingRequirements();

	/* Make sure there's no conflict with the SPI and its contexts. */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	ReplicationSlotCreate(REPL_SLOT_NAME, true, RS_EPHEMERAL);

	/*
	 * Neither prepare_write nor do_write callback is useful for us: we don't
	 * release the slot until done, to prevent anyone from "stealing" changes
	 * from us. Thus no one can use the SQL interface on our slot.
	 */
	ctx = CreateInitDecodingContext(REPL_PLUGIN_NAME, NIL,
									logical_read_local_xlog_page,
									NULL, NULL);
	DecodingContextFindStartpoint(ctx);

	/*
	 * Setup structures to store decoded changes.
	 *
	 * TODO Find out what's wrong about letting the plugin immediately write
	 * into the destination table itself. PG documentation forbids that, but
	 * I'm not sure if that restriction is still valid and why.
	 */
	dstate = palloc0(sizeof(DecodingOutputState));
	dstate->relid = relid;
	dstate->tstore = tuplestore_begin_heap(false, false,
										   maintenance_work_mem);
	dstate->tupdesc = tup_desc;

	/* Initialize the descriptor to store the changes ... */
	dstate->tupdesc_change = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(dstate->tupdesc_change, 1, NULL, BYTEAOID, -1, 0);
	/* ... as well as the corresponding slot. */
	dstate->tsslot = MakeTupleTableSlot();
	ExecSetSlotDescriptor(dstate->tsslot, dstate->tupdesc_change);

	dstate->data_size = 0;
	dstate->resowner = 	ResourceOwnerCreate(CurrentResourceOwner,
											"logical decoding");

	MemoryContextSwitchTo(oldcontext);

	ctx->output_writer_private = dstate;
	return ctx;
}

static void
decoding_cleanup(LogicalDecodingContext *ctx)
{
	DecodingOutputState	*dstate;

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
	HeapTuple	tuple;
	Form_pg_class	form_class;
	Relation	rel;
	TupleDesc	desc;
	SysScanDesc scan;
	ScanKeyData key[1];
	Snapshot	snapshot;
	Datum	options_raw;
	bool	isnull;
	StdRdOptions *reloptions;
	CatalogState	*result;

	/*
	 * ScanPgRelation.c would do most of the work below, but relcache.c does
	 * not export it.
	 */
	rel = heap_open(RelationRelationId, AccessShareLock);
	desc = CreateTupleDescCopy(RelationGetDescr(rel));

	/*
	 * The relation is not a real catalog relation, but GetCatalogSnapshot()
	 * does exactly what we need, i.e. retrieves the most recent snapshot. If
	 * changing this, make sure that isolation level has no impact on the
	 * snapshot "freshness".
	 *
	 * The same snapshot is used for all the catalog scans in this function,
	 * but it's not critical at the moment. If someone manages to change
	 * attribute or index concurrently (shouldn't happen at least for
	 * attributes because AccessShareLock should currently be held on the
	 * source relation), the first call of check_catalog_changes() will reveal
	 * the change.
	 */
	snapshot = GetCatalogSnapshot(relid);

	ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));
	scan = systable_beginscan(rel, ClassOidIndexId, true, snapshot, 1, key);
	tuple = systable_getnext(scan);

	/*
	 * The relation should be locked by caller, so it must not have
	 * disappeared.
	 */
	Assert(HeapTupleIsValid(tuple));

	/* Invalid relfilenode indicates mapped relation. */
	form_class = (Form_pg_class) GETSTRUCT(tuple);
	if (form_class->relfilenode == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 (errmsg("Mapped relation cannot be squeezed"))));

	result = (CatalogState *) palloc0(sizeof(CatalogState));

	/* The "user_catalog_option" is essential. */
	reloptions = (StdRdOptions *) extractRelOptions(tuple, desc, NULL);
	if (reloptions == NULL || !reloptions->user_catalog_table)
	{
		Assert(!result->is_catalog);
		return result;
	}

	/*
	 * If TOAST relation existsq, we must also keep eye on the catalog option.
	 */
	if (form_class->reltoastrelid != InvalidOid)
	{
		HeapTuple	tuple_toast;
		ScanKeyData key_toast[1];
		SysScanDesc	scan_toast;
		StdRdOptions	*reloptions_toast;

		ScanKeyInit(&key_toast[0], ObjectIdAttributeNumber,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(form_class->reltoastrelid));
		scan_toast = systable_beginscan(rel, ClassOidIndexId, true, snapshot,
										1, key_toast);
		tuple_toast = systable_getnext(scan_toast);

		reloptions_toast = (StdRdOptions *) extractRelOptions(tuple_toast,
															  desc, NULL);
		if (reloptions_toast == NULL || !reloptions_toast->user_catalog_table)
		{
			Assert(!result->is_catalog);
			return result;
		}
		result->toast_xmin = HeapTupleHeaderGetXmin(tuple_toast->t_data);
		systable_endscan(scan_toast);
	}

	result->relid = relid;
	result->desc_class = desc;
	result->form_class = (Form_pg_class) palloc(CLASS_TUPLE_SIZE);
	memcpy(result->form_class, form_class, CLASS_TUPLE_SIZE);

	options_raw = fastgetattr(tuple, Anum_pg_class_reloptions, desc, &isnull);
	Assert(!isnull);
	Assert(PointerIsValid(DatumGetPointer(options_raw)));
	result->reloptions = (Datum) PG_DETOAST_DATUM_COPY(options_raw);

	result->is_catalog = true;

	/*
	 * pg_class(xmin) helps to ensure that the "user_catalog_option" wasn't
	 * turned off and on. On the other hand it might restrict some concurrent
	 * DDLs that would be safe as such.
	 */
	result->pg_class_xmin = HeapTupleHeaderGetXmin(tuple->t_data);

	/*
	 * We might want to avoid the check if relhasindex is false, but
	 * index_update_stats() updates this field in-place. (Currently it should
	 * not change from "true" to "false", but let's be cautious anyway.)
	 */
	result->indexes = get_index_info(relid, &result->relninds,
									 &result->invalid_index, false,
									 snapshot);

	/* If any index is "invalid", no more catalog information is needed. */
	if (result->invalid_index)
		return result;

	if (form_class->relnatts > 0)
		result->attr_xmins = get_attribute_xmins(relid, form_class->relnatts,
												 snapshot);

	/* Cleanup. */
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return result;
}

/*
 * Retrieve array of pg_attribute(xmin) values for given relation, ordered by
 * attnum. (The ordering is not essential but lets us do some extra sanity
 * checks.)
 */
static TransactionId *
get_attribute_xmins(Oid relid, int relnatts, Snapshot snapshot)
{
	Relation	rel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	TransactionId	*result;
	int	n = 0;

	rel = heap_open(AttributeRelationId, AccessShareLock);
	if (snapshot == NULL)
		snapshot = GetCatalogSnapshot(relid);
	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	/* System columns should not be ALTERed. */
	ScanKeyInit(&key[1],
				Anum_pg_attribute_attnum,
				BTGreaterStrategyNumber, F_INT2GT,
				Int16GetDatum(0));
	scan = systable_beginscan(rel, AttributeRelidNumIndexId, true, snapshot,
							  2, key);
	result = (TransactionId *) palloc(relnatts * sizeof(TransactionId));
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_attribute	form;
		int	i;

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
	}
	Assert(relnatts == n);
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
	return result;
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
 */
static IndexCatInfo *
get_index_info(Oid relid, int *relninds, bool *found_invalid,
			   bool invalid_check_only, Snapshot snapshot)
{
	Relation	rel;
	ScanKeyData key[1];
	bool		snap_received;
	SysScanDesc scan;
	HeapTuple	tuple;
	IndexCatInfo		*result;
	int	i, n = 0;
	int	relninds_max = 4;
	Datum		*oids_d;
	int16		oidlen;
	bool		oidbyval;
	char		oidalign;
	ArrayType	*oids_a;
	bool		mismatch;

	*found_invalid = false;

	rel = heap_open(IndexRelationId, AccessShareLock);
	if (snapshot != NULL)
		snap_received = true;
	if (!snap_received)
		snapshot = GetCatalogSnapshot(relid);
	ScanKeyInit(&key[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(rel, IndexIndrelidIndexId, true, snapshot,
							  1, key);

	result = (IndexCatInfo *) palloc(relninds_max * sizeof(IndexCatInfo));
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index	form;
		IndexCatInfo	*res_entry;

		form = (Form_pg_index) GETSTRUCT(tuple);

		/*
		 * First, perform the simple checks that can make the next work
		 * unnecessary.
		 */
		if (!IndexIsValid(form) || !IndexIsReady(form) || !IndexIsLive(form))
		{
			*found_invalid = true;
			break;
		}

		res_entry = (IndexCatInfo *) &result[n++];
		res_entry->oid = form->indexrelid;
		res_entry->xmin = HeapTupleHeaderGetXmin(tuple->t_data);

		/*
		 * Unlike get_attribute_xmins(), we can't receive the expected number
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
	heap_close(rel, AccessShareLock);

	/* Return if invalid index was found or ... */
	if (*found_invalid)
		return result;
	/* ... caller is not interested in anything else.  */
	if (invalid_check_only)
		return result;

	/*
	 * Enforce sorting by OID, so that the entries match the result of the
	 * following scan using OID index.
	 */
	qsort(result, n, sizeof(IndexCatInfo), index_cat_info_compare);

	if (relninds)
		*relninds = n;
	if (n == 0)
		return result;

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

	rel = heap_open(RelationRelationId, AccessShareLock);
	if (!snap_received)
		snapshot = GetCatalogSnapshot(relid);
	ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
				F_OIDEQ, PointerGetDatum(oids_a));
	key[0].sk_flags |= SK_SEARCHARRAY;
	scan = systable_beginscan(rel, ClassOidIndexId, true, snapshot, 1, key);
	i = 0;
	mismatch = false;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		IndexCatInfo	*res_item;
		Form_pg_class	form_class;
		char	*namestr;
		Size	namelen;

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
		namelen = strlen(namestr);
		Assert(namelen < NAMEDATALEN);
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
	heap_close(rel, AccessShareLock);
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
 * altered/ dropped, and no heap rewriting took place.
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
	Oid	toast_relid;

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
	check_pg_class_changes(state->relid, state->pg_class_xmin, lock_held);

	/*
	 * If TOAST relation exists, check it too.
	 *
	 * It's questionable whether change of the pg_class of the TOAST relation
	 * should ever be expected. Let's do it to guard user against accidental
	 * misuse of the set_reloptions() function. In contrast, we don't check
	 * changes of TOAST indexes or attributes - these should not happen unless
	 * allow_system_table_mods GUC is set deliberately.
	 */
	toast_relid = state->form_class->reltoastrelid;
	if (OidIsValid(toast_relid))
	{
		/*
		 * Lock on the relation does not imply lock on its TOAST, so assume
		 * NoLock.
		 */
		check_pg_class_changes(toast_relid, state->toast_xmin, NoLock);
	}

	/*
	 * Index change does not necessarily require lock of the parent relation,
	 * so check indexes unconditionally.
	 */
	check_index_changes(state->relid, state->indexes, state->relninds);

	/*
	 * XXX If any lock level lower than AccessExclusiveLock conflicts with all
	 * commands that change pg_attribute catalog, skip this check if lock_held
	 * is at least that level.
	 */
	check_attribute_changes(state->relid, state->attr_xmins,
							state->form_class->relnatts);
}

static void
check_pg_class_changes(Oid relid, TransactionId xmin, LOCKMODE lock_held)
{
	Snapshot	snapshot;
	ScanKeyData key[1];
	HeapTuple	pg_class_tuple = NULL;
	Relation	pg_class_rel;
	TupleDesc	pg_class_desc;
	SysScanDesc pg_class_scan;
	TransactionId		pg_class_xmin;

	/* This part is identical to the beginning of get_catalog_state(). */
	ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));
	pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
	pg_class_desc = CreateTupleDescCopy(RelationGetDescr(pg_class_rel));
	snapshot = GetCatalogSnapshot(relid);
	pg_class_scan = systable_beginscan(pg_class_rel, ClassOidIndexId,
									   true, snapshot, 1, key);
	pg_class_tuple = systable_getnext(pg_class_scan);

	/* As the relation might not be locked, it could have disappeared. */
	if (!HeapTupleIsValid(pg_class_tuple))
	{
		Assert(lock_held == NoLock);

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 (errmsg("Table no longer exists"))));
	}

	/*
	 * Check if pg_class(xmin) has changed. Note that it makes no sense to
	 * check CatalogState.is_catalog here. Even true value does not tell
	 * whether "user_catalog_option" was never changed back and
	 * forth. pg_class(xmin) will reveal any change of the storage option.
	 *
	 * Besides the "user_catalog_option", we use pg_class(xmin) to detect
	 * change of pg_class(relfilenode), which indicates heap rewriting or
	 * TRUNCATE command (or concurrent call of squeeze_table(), but that
	 * should fail to allocate new replication slot). (Invalid relfilenode
	 * does not change, but mapped relations are excluded from processing
	 * by get_catalog_state().)
	 */
	pg_class_xmin = HeapTupleHeaderGetXmin(pg_class_tuple->t_data);
	if (!TransactionIdEquals(pg_class_xmin, xmin))
		/* XXX Does more suitable error code exist? */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Incompatible DDL or heap rewrite performed concurrently")));

	systable_endscan(pg_class_scan);
	heap_close(pg_class_rel, AccessShareLock);
	pfree(pg_class_desc);
}

static void
check_attribute_changes(Oid relid, TransactionId *attrs, int relnatts)
{
	TransactionId	*attrs_new;
	int i;

	/*
	 * Since pg_class should have been checked by now, relnatts can only be
	 * zero if it was zero originally, so there's no info to be compared to
	 * the current state.
	 */
	if (relnatts == 0)
	{
		Assert(attrs == NULL);
		return;
	}

	attrs_new = get_attribute_xmins(relid, relnatts, NULL);
	for (i = 0; i < relnatts; i++)
	{
		if (!TransactionIdEquals(attrs[i], attrs_new[i]))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("Table definition changed concurrently")));
	}
	pfree(attrs_new);
}

static void
check_index_changes(Oid relid, IndexCatInfo *indexes, int relninds)
{
	IndexCatInfo	*inds_new;
	int	relninds_new;
	bool	failed = false;
	bool	invalid_index;

	if (relninds == 0)
	{
		Assert(indexes == NULL);
		return;
	}

	inds_new = get_index_info(relid, &relninds_new, &invalid_index, false,
							  NULL);

	/*
	 * If this field was set to true, no attention was paid to the other
	 * fields during catalog scans.
	 */
	if (invalid_index)
		failed = true;

	if (!failed && relninds_new != relninds)
		failed = true;

	if (!failed)
	{
		int i;

		for (i = 0; i < relninds; i++)
		{
			IndexCatInfo	*ind, *ind_new;

			ind = &indexes[i];
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
free_catalog_state(CatalogState *state)
{
	if (state->form_class)
		pfree(state->form_class);

	if (state->desc_class)
		pfree(state->desc_class);

	if (state->reloptions)
		pfree(DatumGetPointer(state->reloptions));

	if (state->attr_xmins)
		pfree(state->attr_xmins);

	if (state->indexes)
		pfree(state->indexes);
	pfree(state);
}

static void
resolve_index_tablepaces(TablespaceInfo *tbsp_info, CatalogState *cat_state,
						 ArrayType *ind_tbsp_a)
{
	int	*dims, *lb;
	int	i, ndim;
	int16 elmlen;
	bool elmbyval;
	char elmalign;
	Datum	*elements;
	bool	*nulls;
	int	nelems, nentries;

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
		char	*indname, *tbspname;
		int	j;
		Oid	ind_oid, tbsp_oid;
		IndexTablespace	*ind_ts;

		/* Find OID of the index. */
		indname = NameStr(*DatumGetName(elements[2 * i]));
		ind_oid = InvalidOid;
		for (j = 0; j < cat_state->relninds; j++)
		{
			IndexCatInfo	*ind_cat;

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
 * Install the passed historic snapshot or uninstall it if NULL is passed.
 *
 * Caution: Caller is responsible for calling TeardownHistoricSnapshot()
 * anytime ERROR is raised while the historic snapshot is active.
 *
 * TODO
 *
 * If the unused argument of TeardownHistoricSnapshot() is clarified, pass it
 * to this function too.
 */
static void
switch_snapshot(Snapshot snap_hist)
{
	if (snap_hist)
	{
		Assert(!HistoricSnapshotActive());

		/*
		 * "tuplecids" should only be needed by catalog-modifying
		 * transactions, during decoding. Nothing of it is applicable here, so
		 * pass NULL.
		 */
		SetupHistoricSnapshot(snap_hist, NULL);

		/*
		 * We don't use the historic snapshot in the way SPI expects. Avoid
		 * assertion failure in GetTransactionSnapshot() when it's called by
		 * the SPI.
		 */
		Assert(FirstSnapshotSet);
		FirstSnapshotSet = false;
	}
	else
	{
		TeardownHistoricSnapshot(false);
		/* Revert the hack done above. */
		FirstSnapshotSet = true;
	}

	/*
	 * This is rather paranoia than anything else - the perform_initial_load()
	 * uses each snapshot to access different table, and it does not catalog
	 * changes.
	 */
	InvalidateSystemCaches();
}

/*
 * Use snap_hist snapshot to get the relevant data from rel_src and insert it
 * into rel_dst.
 *
 * Caller is responsible for opening and locking both relations.
 */
static void
perform_initial_load(Relation rel_src, RangeVar *cluster_idx_rv,
					 Snapshot snap_hist, Relation rel_dst)
{
	bool	use_sort;
	int	batch_size, batch_max_size;
	Tuplesortstate *tuplesort = NULL;
	Relation	cluster_idx = NULL;
	HeapScanDesc	heap_scan = NULL;
	IndexScanDesc	index_scan = NULL;
	HeapTuple	*tuples = NULL;
	ResourceOwner	res_owner_old, res_owner_plan;
	BulkInsertState bistate;

	/*
	 * The initial load starts by fetching data from the source table and
	 * should not include concurrent changes.
	 */
	switch_snapshot(snap_hist);

	if (cluster_idx_rv != NULL)
	{
		cluster_idx = relation_openrv(cluster_idx_rv, AccessShareLock);

		/*
		 * Use the cluster.c API to check if the index can be used for
		 * clustering.
		 */
		check_index_is_clusterable(rel_src, RelationGetRelid(cluster_idx),
								   false, NoLock);

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
		 * Now use the special resource owner to release those planner
		 * locks. In fact this owner should contain any other resources, that
		 * the planner might have allocated. Release them all, to avoid leak.
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
		heap_scan = heap_beginscan(rel_src, snap_hist, 0, (ScanKey) NULL);
	else
	{
		index_scan = index_beginscan(rel_src, cluster_idx, snap_hist, 0, 0);
		index_rescan(index_scan, NULL, 0, NULL, 0);
	}

	if (use_sort)
		tuplesort = tuplesort_begin_cluster(RelationGetDescr(rel_src),
											cluster_idx, maintenance_work_mem,
											false);

	batch_max_size = 1024;
	if (!use_sort)
		tuples = (HeapTuple *) palloc(batch_max_size * sizeof(HeapTuple));

	/* Expect many insertions. */
	bistate = GetBulkInsertState();

	while (true)
	{
		HeapTuple	tup_in;
		int	i;
		Size	data_size;

		if (!use_sort)
			data_size = 0;

		/* Sorting cannot be split into batches. */
		for (i = 0; use_sort || (data_size / 1024) < maintenance_work_mem;
			 i++)
		{
			tup_in = use_sort || cluster_idx == NULL ?
				heap_getnext(heap_scan, ForwardScanDirection) :
				index_getnext(index_scan, ForwardScanDirection);

			if (tup_in != NULL)
			{
				bool	flattened = false;

				/*
				 * Even though special snapshot is used to retrieve values
				 * from TOAST relation (see toast_fetch_datum), we'd better
				 * flatten the tuple and thus retrieve the TOAST while the
				 * historic snapshot is active. One particular reason is that
				 * tuptoaster.c does access catalog.
				 */
				if (HeapTupleHasExternal(tup_in))
				{
					tup_in = toast_flatten_tuple(tup_in,
												 RelationGetDescr(rel_src));
					flattened = true;
				}

				if (use_sort)
				{
					tuplesort_putheaptuple(tuplesort, tup_in);
					/* tuplesort should have copied the tuple. */
					if (flattened)
						pfree(tup_in);
				}
				else
				{
					if (i == batch_max_size)
					{
						batch_max_size *= 2;
						tuples = (HeapTuple *)
							repalloc(tuples,
									 batch_max_size * sizeof(HeapTuple));
					}
					if (!flattened)
						tup_in = heap_copytuple(tup_in);
					tuples[i] = tup_in;
					data_size += HEAPTUPLESIZE + tup_in->t_len;
				}
			}
			else
				break;
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
		switch_snapshot(NULL);

		if (use_sort)
			tuplesort_performsort(tuplesort);
		else
		{
			/*
			 * It's probably safer not to do this test in the generic case: in
			 * theory, the counter might end up zero as a result of
			 * overflow. (For the unsorted case we assume reasonable batch
			 * size.)
			 */
			if (i == 0)
				break;
		}

		batch_size = i;
		i = 0;
		while (true)
		{
			HeapTuple	tup_out;
			bool	should_free = false;;

			if (use_sort)
				tup_out = tuplesort_getheaptuple(tuplesort, true,
												 &should_free);
			else
			{
				if (i == batch_size)
					tup_out = NULL;
				else
					tup_out = tuples[i++];
			}

			if (tup_out == NULL)
				break;

			heap_insert(rel_dst, tup_out, GetCurrentCommandId(true), 0,
						bistate);
			if (!use_sort || should_free)
				pfree(tup_out);
		}

		if (tup_in == NULL)
			break;

		switch_snapshot(snap_hist);
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
		heap_endscan(heap_scan);
	if (index_scan != NULL)
		index_endscan(index_scan);

	/*
	 * Unlock the index, but not the relation yet - caller will do so when
	 * appropriate.
	 */
	if (cluster_idx != NULL)
		relation_close(cluster_idx, AccessShareLock);
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
	Form_pg_class	form_class;
	Oid	toastrelid;
	Oid	result;

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
		aclresult = pg_tablespace_aclcheck(tablespace, relowner, ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tablespace));
	}
	if (tablespace == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	relname = makeStringInfo();
	appendStringInfo(relname, "tmp_%u", cat_state->relid);

	/*
	 * Constraints are not created because each data change must be committed
	 * in the source table before we see it during initial load or via logical
	 * decoding.
	 */
	form_class = cat_state->form_class;
	result = heap_create_with_catalog(
		relname->data,
		form_class->relnamespace, tablespace,
		InvalidOid, InvalidOid, InvalidOid,
		form_class->relowner, tup_desc, NIL,
		form_class->relkind, form_class->relpersistence,
		false, false, true, 0,
		ONCOMMIT_NOOP, cat_state->reloptions,
		false, false, false, NULL);

	Assert(OidIsValid(result));

	elog(DEBUG1, "Transient relation created: %u", result);

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
		Datum	reloptions;
		bool	is_null;
		HeapTuple	tuple;

		/* keep the existing toast table's reloptions, if any */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastrelid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", toastrelid);
		reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
									 &is_null);
		if (is_null)
			reloptions = (Datum) 0;

		/*
		 * No lock is needed on the target relation - no other transaction
		 * should be able to see it yet.
		 */
		NewHeapCreateToastTable(result, reloptions, NoLock);

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
						TablespaceInfo *tbsp_info, CatalogState *cat_state)
{
	StringInfo	ind_name;
	int	i;
	Oid	*result;

	Assert(nindexes > 0);

	ind_name = makeStringInfo();
	result = (Oid *) palloc(nindexes * sizeof(Oid));

	for (i = 0; i < nindexes; i++)
	{
		Oid	ind_oid, ind_oid_new, tbsp_oid;
		Relation	ind;
		IndexInfo	*ind_info;
		int	j, heap_col_id;
		StringInfo	col_name_buf = NULL;
		List	*colnames;
		int16	indnatts;
		Oid	*collations, *opclasses;
		HeapTuple	ind_tup;
		bool	isnull;
		Datum	d;
		oidvector *oidvec;
		int2vector *int2vec;
		size_t	oid_arr_size;
		size_t	int2_arr_size;
		int16	*indoptions;
		bool	isconstraint;

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
			IndexTablespace	*ind;

			ind = &tbsp_info->indexes[j];
			if (ind->index == ind_oid)
			{
				tbsp_oid = ind->tablespace;
				break;
			}
		}

		if (tbsp_oid == GLOBALTABLESPACE_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only shared relations can be placed in pg_global tablespace")));

		if (!OidIsValid(tbsp_oid))
		{
			bool found = false;

			for (j = 0; j < cat_state->relninds; j++)
			{
				IndexCatInfo	*ind_cat;

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
		index_close(ind, AccessShareLock);

		colnames = NIL;
		indnatts = ind->rd_index->indnatts;
		oid_arr_size = sizeof(Oid) * indnatts;
		int2_arr_size = sizeof(int16) * indnatts;

		collations = (Oid *) palloc(oid_arr_size);
		for (j = 0; j < indnatts; j++)
		{
			char	*colname;

			heap_col_id = ind->rd_index->indkey.values[j];
			if (heap_col_id > 0)
			{
				Form_pg_attribute	att;

				/* Normal attribute. */
				att = rel_src->rd_att->attrs[heap_col_id - 1];
				colname = pstrdup(NameStr(att->attname));
				collations[j] = att->attcollation;
			}
			else if (heap_col_id == 0)
			{
				HeapTuple	tuple;
				Form_pg_attribute	att;

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
			else if (heap_col_id == ObjectIdAttributeNumber)
			{
				/*
				 * OID should be expected because of OID indexes, however user
				 * can use the OID column in arbitrary index. Therefore we'd
				 * better generate an unique column name.
				 *
				 * XXX Is it worth checking that the index satisfies other
				 * characteristics of an OID index?
				 */
				if (col_name_buf == NULL)
					col_name_buf = makeStringInfo();
				else
					resetStringInfo(col_name_buf);
				appendStringInfo(col_name_buf, "oid_%d", j);
				colname = pstrdup(col_name_buf->data);
				collations[j] = InvalidOid;
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
		ind_tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(ind_oid));
		if (!HeapTupleIsValid(ind_tup))
			elog(ERROR, "cache lookup failed for index %u", ind_oid);
		d = SysCacheGetAttr(INDEXRELID, ind_tup, Anum_pg_index_indclass,
							&isnull);
		Assert(!isnull);
		oidvec = (oidvector *) DatumGetPointer(d);
		opclasses = (Oid *) palloc(oid_arr_size);
		memcpy(opclasses, oidvec->values, oid_arr_size);

		d = SysCacheGetAttr(INDEXRELID, ind_tup, Anum_pg_index_indoption,
							&isnull);
		Assert(!isnull);
		int2vec = (int2vector *) DatumGetPointer(d);
		indoptions = (int16 *) palloc(int2_arr_size);
		memcpy(indoptions, int2vec->values, int2_arr_size);

		ReleaseSysCache(ind_tup);

		isconstraint = ind->rd_index->indisprimary || ind_info->ii_Unique
			|| ind->rd_index->indisexclusion;

		ind_oid_new = index_create(rel_dst, ind_name->data,
								   InvalidOid, InvalidOid, ind_info,
								   colnames, ind->rd_rel->relam,
								   tbsp_oid,
								   collations, opclasses, indoptions,
								   PointerGetDatum(ind->rd_options),
								   ind->rd_index->indisprimary, isconstraint,
								   false, false, false, false, false, false,
								   false);
		result[i] = ind_oid_new;

		list_free_deep(colnames);
		pfree(collations);
		pfree(opclasses);
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
	Form_pg_index	ident_idx;
	int	n, i;
	ScanKey	result;

	Assert(OidIsValid(ident_idx_oid));
	ident_idx_rel = index_open(ident_idx_oid, AccessShareLock);
	ident_idx = ident_idx_rel->rd_index;
	n = ident_idx->indnatts;
	result = (ScanKey) palloc(sizeof(ScanKeyData) * n);
	for (i = 0; i < n; i++)
	{
		ScanKey	entry;
		int16	relattno;
		TupleDesc	desc;
		Form_pg_attribute	att;
		Oid	opfamily, opno, opcode;

		entry = &result[i];
		relattno = ident_idx->indkey.values[i];
		desc = rel_src->rd_att;
		att = desc->attrs[relattno - 1];
		opfamily = ident_idx_rel->rd_opfamily[i];
		opno = get_opfamily_member(opfamily, att->atttypid, att->atttypid,
								   BTEqualStrategyNumber);
		if (!OidIsValid(opno))
			elog(ERROR, "Failed to find = operator for type %u",
				 att->atttypid);

		opcode = get_opcode(opno);
		if (!OidIsValid(opcode))
			elog(ERROR, "Failed to find = operator for operator %u", opno);

		/* Initialize everything but argument. */
		ScanKeyInit(entry,
					relattno,
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
					LogicalDecodingContext *ctx, XLogRecPtr	*startptr)
{
	bool	success;
	XLogRecPtr	xlog_insert_ptr, end_of_wal;
	int	i;
	struct timeval t_end;
	struct timeval *t_end_ptr = NULL;

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
		int64 usec;
		struct timeval t_start;

		gettimeofday(&t_start, NULL);
		usec = t_start.tv_usec + 1000 * (squeeze_max_xlock_time % 1000);
		t_end.tv_sec = t_start.tv_sec + usec / USECS_PER_SEC;
		t_end.tv_usec = usec % USECS_PER_SEC;
		t_end_ptr = &t_end;
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
	 * available for decoding. (This should be unnecessary if all backends had
	 * synchronous_commit set, but we can't rely on this setting.)
	 */
	xlog_insert_ptr = GetInsertRecPtr();
	XLogFlush(xlog_insert_ptr);
	end_of_wal = GetFlushRecPtr();

	/*
	 * Process the changes that might have taken place while we were waiting
	 * for the lock.
	 *
	 * AccessExclusiveLock effectively disables catalog checks - we've already
	 * performed them above.
	 */
	success = process_concurrent_changes(ctx, startptr, end_of_wal,
										 cat_state, rel_dst, ident_key,
										 ident_key_nentries, iistate,
										 AccessExclusiveLock, t_end_ptr);
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
		process_concurrent_changes(ctx, startptr, end_of_wal,
								   cat_state, rel_dst, ident_key,
								   ident_key_nentries, iistate,
								   AccessExclusiveLock, NULL);
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
 *
 * TODO Adopt parts of finish_heap_swap(), especially renaming the TOAST
 * tables.
 *
 * TODO Consider if InvokeObjectPostAlterHookArg() should be called on
 * r1. Which call sites in PG core consider themselves as ALTER?
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
	relRelation = heap_open(RelationRelationId, RowExclusiveLock);

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

		/*
		 * Although this condition shouldn't really occur, elog seems wiser
		 * here than Assert() in this case.
		 */
		if (relform1->relpersistence != relform1->relpersistence)
			elog(ERROR, "relpersistence does not match");

		swaptemp = relform1->reltoastrelid;
		relform1->reltoastrelid = relform2->reltoastrelid;
		relform2->reltoastrelid = swaptemp;
	}
	else
		elog(ERROR, "cannot swap mapped relations");

	/*
	 * Set rel1's frozen Xid and minimum MultiXid so that they become the
	 * lower bounds on XID.
	 *
	 * It'd probably be correct to copy the values from the original table,
	 * but that would leave the tuples too far in the past. Thus VACUUM could
	 * get overly eager about wrap-around avoidance (possibly including
	 * unnecessary full scans), but would find very little work to do.
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

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32		swap_pages;
		float4		swap_tuples;
		int32		swap_allvisible;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;
	}

	simple_heap_update(relRelation, &reltup1->t_self, reltup1);
	simple_heap_update(relRelation, &reltup2->t_self, reltup2);

	indstate = CatalogOpenIndexes(relRelation);
	CatalogIndexInsert(indstate, reltup1);
	CatalogIndexInsert(indstate, reltup2);
	CatalogCloseIndexes(indstate);

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

	heap_close(relRelation, RowExclusiveLock);

	RelationCloseSmgrByOid(r1);
	RelationCloseSmgrByOid(r2);
}

/*
 * Swap TOAST relation names if needed.
 *
 * relid1 represents the relation to stay and toastrelid1 its TOAST relation.
 * relid2 refer to the transient relation in the same manner.
 *
 * The storage of TOAST tables and their indexes have already been swapped.
 *
 * On exit we hold AccessExclusiveLock on the TOAST relations and their
 * indexes.
 */
static void
swap_toast_names(Oid relid1, Oid toastrelid1, Oid relid2, Oid toastrelid2)
{
	char	name[NAMEDATALEN];
	Oid toastidxid;

	/*
	 * As we haven't changed tuple descriptor, both relation do or both do not
	 * have TOAST - see toasting.c:needs_toast_table().
	 */
	if (!OidIsValid(toastrelid1))
	{
		if (OidIsValid(toastrelid2))
			elog(ERROR, "Unexpected TOAST relation exists");
		return;
	}
	if (!OidIsValid(toastrelid2))
		elog(ERROR, "Missing TOAST relation");

	/*
	 * Added underscore should be enough to keep names unique (at least within
	 * the pg_toast tablespace). This assumption makes name retrieval
	 * unnecessary.
	 */
	snprintf(name, NAMEDATALEN, "pg_toast_%u_", relid1);
	RenameRelationInternal(toastrelid2, name, true);

	/*
	 * XXX While toast_open_indexes (PG core) can retrieve multiple indexes,
	 * get_toast_index() expects exactly one. If this restriction should be
	 * released someday, either generate the underscore-terminated names as
	 * above or copy names of the indexes of toastrel1 (the number of indexes
	 * should be identical). Order should never be important, as toastrel2
	 * will eventually be dropped.q
	 */
	toastidxid = get_toast_index(toastrelid2);
	snprintf(name, NAMEDATALEN, "pg_toast_%u_index_", relid1);
	RenameRelationInternal(toastidxid, name, true);
	CommandCounterIncrement();

	/* Now set the desired names on the TOAST stuff of relid1. */
	snprintf(name, NAMEDATALEN, "pg_toast_%u", relid1);
	RenameRelationInternal(toastrelid1, name, true);
	toastidxid = get_toast_index(toastrelid1);
	snprintf(name, NAMEDATALEN, "pg_toast_%u_index", relid1);
	RenameRelationInternal(toastidxid, name, true);
	CommandCounterIncrement();

	/* Do not bother to fix names for relid2 - they'll be dropped anyway. */
}

/*
 * The function is called after RenameRelationInternal() which does not
 * release AccessExclusiveLock.
 */
static Oid
get_toast_index(Oid toastrelid)
{
	Relation	toastrel;
	List	*toastidxs;
	Oid	result;

	toastrel = heap_open(toastrelid, NoLock);
	toastidxs = RelationGetIndexList(toastrel);

	if (toastidxs == NIL || list_length(toastidxs) != 1)
		elog(ERROR, "Unexpected number of TOAST indexes");

	result = linitial_oid(toastidxs);
	heap_close(toastrel, NoLock);

	return result;
}

/*
 * Set or clear "user_catalog_table" storage option as well
 * "autovacuum_enabled" option (the latter also for TOAST relation if it
 * exists).
 *
 * ALTER TABLE command can do this for user table, but not for TOAST (unless
 * "allow_system_table_mods" is set, which we consider dangerous).
 *
 * By changing all the settings all at once we avoid repeated exclusive
 * locking.
 */
extern Datum set_reloptions(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_reloptions);
Datum
set_reloptions(PG_FUNCTION_ARGS)
{
	Name	relschema, relname;
	bool	user_cat, autovac, autovac_toast;
	RangeVar   *relrv;
	Relation	rel;
	Form_pg_class	rel_form;
	Oid	relid, toastrelid;

	/*
	 * Check all arguments explicitly. We could define the function as strict,
	 * but that would silently ignore incorrect invocation.
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 (errmsg("All arguments must be specified"))));

	relschema = PG_GETARG_NAME(0);
	relname = PG_GETARG_NAME(1);
	user_cat = PG_GETARG_BOOL(2);
	autovac = PG_GETARG_BOOL(3);
	autovac_toast = PG_GETARG_BOOL(4);

	relrv = makeRangeVar(NameStr(*relschema), NameStr(*relname), -1);
	rel = heap_openrv(relrv, AccessExclusiveLock);
	relid = RelationGetRelid(rel);

	update_reloptions(relid, user_cat, autovac);

	/* Handle TOAST relation if there's one. */
	Assert(!OidIsValid(rel->rd_toastoid));
	rel_form = RelationGetForm(rel);
	toastrelid = rel_form->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		Relation	toastrel;

		toastrel = heap_open(toastrelid, AccessExclusiveLock);
		update_reloptions(toastrelid, user_cat, autovac_toast);
		heap_close(toastrel, AccessExclusiveLock);
	}

	heap_close(rel, AccessExclusiveLock);
	PG_RETURN_VOID();
}


/*
 * Update pg_class(reloptions) according to "changes".
 *
 * Caller should hold AccessExclusiveLock on the relation whose pg_class(id)
 * he passes.
 */
static void
update_reloptions(Oid relid, bool user_cat, bool autovac)
{
	Relation	rel;
	HeapTuple	tuple, tuple_new;
	TupleDesc	desc;
	int	natts;
	Datum	*repl_vals;
	bool	*repl_nulls, *do_repl;
	Datum	options;
	bool	isnull;
	List	*changes_reset, *changes_set;
	DefElem	*def;

	rel = heap_open(RelationRelationId, RowExclusiveLock);
	desc = RelationGetDescr(rel);
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	Assert(HeapTupleIsValid(tuple));

	options = fastgetattr(tuple, Anum_pg_class_reloptions, desc, &isnull);
	if (PointerIsValid(DatumGetPointer(options)))
		options = (Datum) PG_DETOAST_DATUM(options);
	else
		options = (Datum) 0;

	changes_reset = NIL;
	changes_set = NIL;
	if (user_cat)
	{
		def = makeDefElem("user_catalog_table", (Node *) makeString("true"));
		changes_set = lappend(changes_set, def);
	}
	else
	{
		def = makeDefElem("user_catalog_table", NULL);
		changes_reset = lappend(changes_reset, def);
	}

	/*
	 * Default value of "autovacuum_enabled" is "true", so only keep it in the
	 * list if user requires "false".
	 */
	if (!autovac)
	{
		def = makeDefElem("autovacuum_enabled", (Node *) makeString("false"));
		changes_set = lappend(changes_set, def);
	}
	else
	{
		def = makeDefElem("autovacuum_enabled", NULL);
		changes_reset = lappend(changes_reset, def);
	}

	/* Apply the changes */
	if (list_length(changes_reset) > 0)
		options = transformRelOptions(options, changes_reset, NULL, NULL,
									  false, true);

	if (list_length(changes_set) > 0)
		options = transformRelOptions(options, changes_set, NULL, NULL,
									  false, false);

	natts = desc->natts;
	repl_vals = (Datum *) palloc(natts * sizeof(Datum));
	repl_nulls = (bool *) palloc0(natts * sizeof(bool));
	do_repl = (bool *) palloc0(natts * sizeof(bool));

	if (options != (Datum) 0)
		repl_vals[Anum_pg_class_reloptions - 1] = options;
	else
		repl_nulls[Anum_pg_class_reloptions - 1] = true;
	do_repl[Anum_pg_class_reloptions - 1] = true;

	/* Store the change. */
	tuple_new = heap_modify_tuple(tuple, desc, repl_vals, repl_nulls,
								  do_repl);
	simple_heap_update(rel, &tuple_new->t_self, tuple_new);
	CatalogUpdateIndexes(rel, tuple_new);

	/* Cleanup. */
	ReleaseSysCache(tuple);
	heap_freetuple(tuple_new);
	heap_close(rel, RowExclusiveLock);
}

/*
 * Retrieve value of "autovacuum_enabled" option of a table specified by OID.
 */
extern Datum is_autovacuum_enabled(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(is_autovacuum_enabled);
Datum
is_autovacuum_enabled(PG_FUNCTION_ARGS)
{
	Oid	relid;
	HeapTuple	tuple;
	bool	is_null;
	Form_pg_class	class_form;
	Datum	reloptions_d;
	StdRdOptions *reloptions;
	bool	result;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 (errmsg("OID must be specified"))));

	relid = PG_GETARG_OID(0);

	/*
	 * It's legal for OID to be invalid if table has no TOAST relation. Return
	 * value doesn't really make sense in such case. Return "true" because
	 * it's the default of "autovacuum_enabled".
	 */
	if (!OidIsValid(relid))
		PG_RETURN_BOOL(true);

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	class_form = (Form_pg_class) GETSTRUCT(tuple);
	reloptions_d = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								 &is_null);

	if (is_null)
		/* Default value. */
		result = true;
	else
	{
		reloptions = (StdRdOptions *) heap_reloptions(class_form->relkind,
													  reloptions_d, false);
		result = reloptions->autovacuum.enabled;
	}
	ReleaseSysCache(tuple);

	PG_RETURN_BOOL(result);
}
