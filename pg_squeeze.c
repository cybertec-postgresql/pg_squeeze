#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/snapbuild.h"
#include "storage/lmgr.h"
#include "storage/standbydefs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "pg_squeeze.h"

PG_MODULE_MAGIC;

#define	REPL_SLOT_NAME		"pg_squeeze_slot"
#define	REPL_PLUGIN_NAME	"pg_squeeze"

/*
 * Information on source relation index, used to build the index on the
 * transient relation. To avoid repeated retrieval of the pg_index fields we
 * also add pg_class(xmin) and pass the same structure to
 * check_catalog_changes().
 */
typedef struct IndexCatInfo
{
	Oid	oid;					/* pg_index(indexrelid) */
	TransactionId		xmin;	/* pg_index(xmin) */
	TransactionId		pg_class_xmin; /* pg_class(xmin) of the index (not the
										* parent relation) */
} IndexCatInfo;

static int index_cat_info_compare(const void *arg1, const void *arg2);

/*
 * Information to check whether an "incompatible" catalog change took
 * place. Such a change prevents us from completing processing of the current
 * table.
 */
typedef struct CatalogState
{
	/* The relation whose changes we'll check for. */
	Oid	relid;

	/* Has the source relation the "user_catalog_table" option set? */
	bool		is_catalog;

	/* pg_class(relnatts) */
	int16		relnatts;
	/* Array of pg_attribute(xmin). (Dropped columns are here too.) */
	TransactionId	*attr_xmins;

	/* Likewise, per-index info. */
	int		relninds;
	IndexCatInfo	*indexes;

	/*
	 * xmin of the pg_class tuple of the source relation during the initial
	 * check.
	 */
	TransactionId		pg_class_xmin;

	/*
	 * Does at least one index wrong value of indisvalid, indisready or
	 * indislive?
	 */
	bool	invalid_index;
} CatalogState;

static LogicalDecodingContext *setup_decoding(void);
static CatalogState *get_catalog_state(Oid relid);
static TransactionId *get_attribute_xmins(Oid relid, int relnatts,
										  Snapshot snapshot);
static IndexCatInfo *get_index_info(Oid relid, int *relninds,
									bool *found_invalid, bool invalid_check_only,
									Snapshot snapshot);
static void check_catalog_changes(CatalogState *state, LOCKMODE lock_held);
static void check_attribute_changes(Oid relid, TransactionId	*attrs, int relnatts);
static void check_index_changes(Oid relid, IndexCatInfo *indexes,
								int relninds);
static void free_catalog_state(CatalogState *state);
static void check_pg_class_changes(Oid relid, TransactionId xmin,
								   LOCKMODE lock_held);
static char *get_column_list(SPITupleTable *cat_data);
static void switch_snapshot(Snapshot snap_hist);
static void perform_initial_load(Relation rel_src, Oid cluster_idx_id,
								 Snapshot snap_hist, Relation rel_dst);
static void decode_concurrent_changes(LogicalDecodingContext *ctx,
									  XLogRecPtr *startptr,
									  XLogRecPtr end_of_wal);
static void process_concurrent_changes(DecodingOutputState *s,
									   Relation relation, ScanKey key,
									   int nkeys, Oid *indexes, int nindexes,
									   Oid ident_index);
static Oid *build_transient_indexes(Relation rel_dst, Relation rel_src,
									Oid *indexes_src, int nindexes);
static ScanKey build_identity_key(Oid ident_idx_oid, Relation rel_src,
								  int *nentries);
static void update_indexes(Relation heap, HeapTuple tuple, Oid *indexes,
						   int nindexes);
static void swap_relation_files(Oid r1, Oid r2);

/*
 * Char attribute to construct tuple descriptor for
 * DecodingOutputState.metadata.
 */
static FormData_pg_attribute att_char_data = {
	0, {"change_kind"}, CHAROID, 0, sizeof(char),
	1, 0, -1, -1,
	true, 'p', 'c', true, false, false, true, 0, InvalidOid
};

static Form_pg_attribute att_char = &att_char_data;

/*
 * SQL interface to squeeze one table interactively.
 *
 * "user_catalog_table" option must have been set by a separate transaction
 * (otherwise the replication slot causes ERROR instead of infinite waiting
 * for consistent state). We try to clear it as soon as possible. Caller
 * should check (and possibly clear) the option if the function failed in any
 * way.
 */
/*
 * TODO
 *
 * 1. Move the logic into a separate function so that background worker can
 * call it w/o FMGR.
 *
 * 2. Return status code instead of raising / not-raising ERROR from the
 * function itself and subroutines. (Never forget to uninstall the historic
 * snapshot.)
 */
extern Datum squeeze_table(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(squeeze_table);
Datum
squeeze_table(PG_FUNCTION_ARGS)
{
	text	   *relname;
	RangeVar   *relrv_src, *relrv_dst;
	Relation	rel_src, rel_dst;
	Oid	ident_idx_src, ident_idx_dst;
	Oid	relid_src, relid_dst;
	Oid	cluster_idx_id;
	char	replident;
	ScanKey	ident_key;
	int	i, ident_key_nentries;
	LogicalDecodingContext	*ctx;
	Snapshot	snap_hist;
	StringInfo	relname_tmp, stmt;
	char	*relname_src, *relname_dst;
	bool	src_has_oids;
	int	spi_res;
	TupleDesc	tup_desc;
	CatalogState		*cat_state;
	DecodingOutputState	*dstate;
	XLogRecPtr	end_of_wal, startptr;
	ResourceOwner resowner_old, resowner_decode;
	XLogRecPtr	xlog_insert_ptr;
	int	nindexes;
	Oid	*indexes_src = NULL, *indexes_dst = NULL;
	bool	invalid_index = false;
	IndexCatInfo	*ind_info;

	relname = PG_GETARG_TEXT_P(0);
	relrv_src = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	/* TODO Consider heap_open() / heap_close(). */
	rel_src = relation_openrv(relrv_src, AccessShareLock);

	RelationGetIndexList(rel_src);
	replident = rel_src->rd_rel->relreplident;
	/*
	 * Save the identity index OID so that we don't have to call
	 * RelationGetIndexList later again.
	 */
	ident_idx_src = rel_src->rd_replidindex;

	/* TODO Accept the clustering index as function argument. ERROR if
	 * rd_rel->relam != BTREE_AM_OID. */
	cluster_idx_id = ident_idx_src;

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
				 (errmsg("Table is not selective"))));

	/* Change processing w/o index is not a good idea. */
	if (replident == REPLICA_IDENTITY_FULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Replica identity \"full\" not supported"))));

	/*
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
	relid_src = rel_src->rd_id;

	/*
	 * Info to initialize tuple slot to retrieve tuples from tuplestore during
	 * the initial load.
	 */
	/* TODO Use check_catalog_changes() to retrieve (a local copy) this
	 * info. */
	tup_desc = CreateTupleDescCopy(RelationGetDescr(rel_src));

	/* Get ready for the subsequent calls of check_catalog_changes(). */
	cat_state = get_catalog_state(relid_src);

	/* Give up if it's clear enough to do. */
	if (!cat_state->is_catalog)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("\"user_catalog_table\" option not set"))));
	if (cat_state->invalid_index)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("At least one index is invalid"))));

	/* Valid identity index should exist now. */
	Assert(OidIsValid(ident_idx_src));

	/* Therefore the total number of indexes is non-zero. */
	nindexes = cat_state->relninds;
	Assert(nindexes > 0);

	/* Copy the OIDs into a separate array, for convenient use later. */
	indexes_src = (Oid *) palloc(nindexes * sizeof(Oid));
	for (i = 0; i < nindexes; i++)
		indexes_src[i] = cat_state->indexes[i].oid;

	src_has_oids = rel_src->rd_rel->relhasoids;

	/*
	 * The relation shouldn't be locked during slot setup. Otherwise another
	 * transaction could write XLOG records before the slots' data.restart_lsn
	 * and we'd have to wait for it to finish. If such a transaction requested
	 * exclusive lock on our relation (e.g. ALTER TABLE), it'd result in a
	 * deadlock.
	 *
	 * We can't keep the lock till the end of transaction anyway - that's why
	 * check_catalog_changes() exists.
	 */
	relation_close(rel_src, AccessShareLock);

	ctx = setup_decoding();

	/*
	 * The first consistent snapshot should be available now. There's no
	 * reason to look for another snapshot for the "main query".
	 *
	 * XXX The 2nd argument is currently unused in PG core.
	 */
	snap_hist = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder,
											InvalidTransactionId);

	switch_snapshot(snap_hist);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Create a new table.
	 *
	 * CREATE TABLE ... LIKE ... would be great but the command is implemented
	 * in a way that does not like historic snapshot (i.e. it can't see the
	 * newly created relation). Therefore we need to construct the DDL
	 * "manually" and execute with the existing transaction snapshot.
	 *
	 * The historic snapshot is still used to retrieve the current table
	 * definition. Thus if anyone changes the table concurrently, he'll
	 * (correctly) cause ERROR during decoding later.
	 */
	relname_src = quote_qualified_identifier(relrv_src->schemaname,
											 relrv_src->relname);
	relname_tmp = makeStringInfo();
	appendStringInfo(relname_tmp, "tmp_%u", relid_src);
	relname_dst = quote_qualified_identifier(relrv_src->schemaname,
											 relname_tmp->data);

	stmt = makeStringInfo();
	appendStringInfo(stmt,
					 "SELECT a.attname, a.attisdropped, "
					 "  pg_catalog.format_type(a.atttypid, NULL)"
					 "FROM pg_catalog.pg_attribute a "
					 "WHERE a.attrelid = %u AND a.attnum > 0 "
					 "ORDER BY a.attnum", relid_src);

	if ((spi_res = SPI_exec(stmt->data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "Failed to get definition of table %s (%d)",
			 relname_dst, spi_res);

	/*
	 * If user is removing columns concurrently, it'd cause ERROR during the
	 * subsequent decoding anyway.
	 */
	/*
	 * TODO If the number of columns is non-zero, check if there's at least
	 * one valid (not dropped).
	 */
	if (SPI_processed < 1)
		/* XXX Try to find better error code. */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 (errmsg("Table %s has no valid columns",
						 relname_src))));

	/*
	 * Construct CREATE TABLE command.
	 *
	 * Constraints are not created because each data change must be committed
	 * in the source table before we see it during initial load or via logical
	 * decoding.
	 */
	resetStringInfo(stmt);
	appendStringInfo(stmt, "CREATE TABLE %s ", relname_dst);
	appendStringInfo(stmt, "(%s)", get_column_list(SPI_tuptable));
	if (src_has_oids)
		appendStringInfoString(stmt, " WITH OIDS");

	/* Back to presence. */
	switch_snapshot(NULL);

	/* Create the transient table. */
	if ((spi_res = SPI_exec(stmt->data, 0)) != SPI_OK_UTILITY)
		elog(ERROR, "Failed to create transient table %s (%d)",
			 relname_dst, spi_res);

	/*
	 * No lock is needed on the target relation - no other transaction should
	 * be able to see it yet.
	 */
	relrv_dst = makeRangeVar(relrv_src->schemaname, relname_tmp->data, -1);
	rel_dst = relation_openrv(relrv_dst, NoLock);
	relid_dst = rel_dst->rd_id;

	/* The source relation will be needed for the initial load. */
	rel_src = heap_open(relid_src, AccessShareLock);

	/*
	 * We need at least to know whether the catalog option was never changed,
	 * and that no DDL took place that allows for data inconsistency. That
	 * includes removal of the "user_catalog_table" option.
	 *
	 * The relation was unlocked for some time since last check, so pass
	 * NoLock.
	 */
	check_catalog_changes(cat_state, NoLock);

	perform_initial_load(rel_src, cluster_idx_id, snap_hist, rel_dst);

	/*
	 * Make sure the logical changes can UPDATE existing rows of the target
	 * table.
	 */
	CommandCounterIncrement();

	/*
	 * Flush all WAL records inserted so far (possibly except for the last
	 * incomplete page, see GetInsertRecPtr), to minimize the amount of data
	 * we need to flush while holding exclusive lock on the source table.
	 */
	xlog_insert_ptr = GetInsertRecPtr();
	XLogFlush(xlog_insert_ptr);

	/*
	 * Decode the data changes that occurred while the initial load was in
	 * progress, and put them into a tuplestore. This is derived from
	 * pg_logical_slot_get_changes_guts().
	 *
	 * TODO Find out what's wrong about letting the plugin immediately write
	 * into the destination table itself. PG documentation forbids that, but
	 * I'm not sure if that restriction is still valid and why.
	 */
	dstate = palloc0(sizeof(DecodingOutputState));
	dstate->relid = relid_src;
	dstate->data.tupstore = tuplestore_begin_heap(false, false, work_mem);
	dstate->data.tupdesc = tup_desc;
	dstate->metadata.tupstore = tuplestore_begin_heap(false, false, work_mem);
	dstate->metadata.tupdesc = CreateTupleDesc(1, false, &att_char);

	end_of_wal = GetFlushRecPtr();
	ctx->output_writer_private = dstate;
	startptr = MyReplicationSlot->data.restart_lsn;

	resowner_old = CurrentResourceOwner;
	CurrentResourceOwner = resowner_decode =
		ResourceOwnerCreate(CurrentResourceOwner, "logical decoding");
	InvalidateSystemCaches();
	decode_concurrent_changes(ctx, &startptr, end_of_wal);
	CurrentResourceOwner = resowner_old;

	/*
	 * Check for concurrent changes that would make us stop working later.
	 * Index build can take quite some effort and we don't want to waste it.
	 *
	 * Note: The reason we haven't released the relation lock immediately
	 * after perform_initial_load() is that the lock might make catalog checks
	 * easier, if check_catalog_changes() gets improved someday. However, as
	 * we need to release the lock later anyway, the current lock does not
	 * prevent already waiting backends from breaking our work. That's why we
	 * don't spend extra effort to lock indexes of the source relation.
	 */
	check_catalog_changes(cat_state, AccessShareLock);

	/*
	 * Create indexes on the temporary table - that might take a
	 * while. (Unlike the concurrent changes, which we insert into existing
	 * indexes.)
	 */
	indexes_dst = build_transient_indexes(rel_dst, rel_src, indexes_src,
										  nindexes);

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

	/*
	 * Build scan key that we'll use to look for rows to be updated / deleted
	 * during logical decoding.
	 */
	ident_key = build_identity_key(ident_idx_src, rel_src,
								   &ident_key_nentries);

	/*
	 * Since the build of indexes could have taken relatively long time, it's
	 * worth checking for concurrent changes again. The point is that the
	 * first batch of concurrent changes can be big (it contains the changes
	 * that took place during the initial load), changes again, so failed
	 * check can prevent us from doing significant work needlessly.
	 *
	 * (We hold share lock on the source relation, but that does not conflict
	 * with some commands, e.g. ALTER INDEX.)
	 */
	check_catalog_changes(cat_state, AccessShareLock);

	/* Process the first batch of concurrent changes. */
	process_concurrent_changes(dstate, rel_dst, ident_key, ident_key_nentries,
							   indexes_dst, nindexes, ident_idx_dst);
	tuplestore_clear(dstate->data.tupstore);
	tuplestore_clear(dstate->metadata.tupstore);

	/*
	 * Before we request exclusive lock, release the shared one acquired for
	 * the initial load. Otherwise we risk a deadlock if some other
	 * transaction holds shared lock and tries to get exclusive one at the
	 * moment.
	 *
	 * As we haven't changed the catalog entry yet, there's no need to send
	 * invalidation messages.
	 */
	heap_close(rel_src, AccessShareLock);

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
	ind_info = get_index_info(relid_src, NULL, &invalid_index, true,
							  NULL);
	if (invalid_index)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));
	else
		pfree(ind_info);

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
	/*
	 * TODO As the index build could have taken long time, consider flushing
	 * and decoding (or even catalog check and processing?) as much XLOG as we
	 * can, so that less work is left to the exclusive lock time.
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

	/*
	 * Check the source relation for DDLs once again. If this check passes, no
	 * DDL can break the process anymore. NoLock must be passed because the
	 * relation was really unlocked for some period since the last check.
	 */
	/*
	 * TODO Consider if this special case requires locking the relevant
	 * catalog tables in front of the LockRelationOid() above and moving the
	 * lock / unlock code into separate functions. If lock on catalog relation
	 * is kept while accessing user relation, deadlock can occur.
	 */
	check_catalog_changes(cat_state, NoLock);
	/* This was the last check. */
	free_catalog_state(cat_state);

	/*
	 * Flush anything we see in WAL, to make sure that all changes committed
	 * while we were creating indexes and waiting for the exclusive lock are
	 * available for decoding. (This should be unnecessary if
	 * synchronous_commit is set, but we can't rely on this setting.)
	 */
	xlog_insert_ptr = GetInsertRecPtr();
	XLogFlush(xlog_insert_ptr);
	end_of_wal = GetFlushRecPtr();

	/*
	 * Decode the changes that might have taken place while we were waiting
	 * for the lock.
	 */
	resowner_old = CurrentResourceOwner;
	CurrentResourceOwner = resowner_decode;
	InvalidateSystemCaches();
	decode_concurrent_changes(ctx, &startptr, end_of_wal);
	CurrentResourceOwner = resowner_old;

	/* Process the second batch of concurrent changes. */
	process_concurrent_changes(dstate, rel_dst, ident_key, ident_key_nentries,
							   indexes_dst, nindexes, ident_idx_dst);

	pfree(ident_key);
	FreeTupleDesc(tup_desc);
	tuplestore_end(dstate->data.tupstore);
	FreeTupleDesc(dstate->metadata.tupdesc);
	tuplestore_end(dstate->metadata.tupstore);

	/* The destination table is no longer necessary, so close it. */
	/* XXX (Should have been closed right after
	 * process_concurrent_changes()?) */
	relation_close(rel_dst, NoLock);

	/*
	 * Exchange storage and indexes between the source and destination
	 * tables.
	 */
	swap_relation_files(relid_src, relid_dst);
	for (i = 0; i < nindexes; i++)
		swap_relation_files(indexes_src[i], indexes_dst[i]);
	if (nindexes > 0)
	{
		pfree(indexes_src);
		pfree(indexes_dst);
	}

	/*
	 * Won't need the slot anymore.
	 *
	 * XXX The SPI calls below seem to touch (free?) the current memory
	 * context. By hiding the current context from those calls we make
	 * postponing of the cleanup possible, but we need to keep the exclusive
	 * lock on the relation till the end of transaction anyway, so it wouldn't
	 * improve concurrency anyway.
	 */
	FreeDecodingContext(ctx);
	ReplicationSlotRelease();

	/*
	 * The "user_catalog_table" option can be cleared now, but we don't want
	 * to commit the current transaction. Try to find out if it can be done in
	 * some kind of autonomous transaction (can we ask bgworker to do so at a
	 * well-defined moment?).
	 *
	 * XXX Besides the commit problem, we can hardly do this earlier because
	 * exclusive lock on relation is kept till commit, so we'd block
	 * concurrent changes. OTOH it's probably not too urgent to relax vacuum
	 * constraint for the "reorganized" table - that shouldn't need vacuum for
	 * some time.
	 */
	resetStringInfo(stmt);
	appendStringInfo(stmt, "ALTER TABLE %s SET (user_catalog_table=false)",
					 relname_src);

	if ((spi_res = SPI_exec(stmt->data, 0)) != SPI_OK_UTILITY)
		elog(ERROR, "Failed to set \"user_catalog_table\" option (%d)",
			 spi_res);

	resetStringInfo(stmt);
	appendStringInfo(stmt, "DROP TABLE %s", relname_dst);

	if ((spi_res = SPI_exec(stmt->data, 0)) != SPI_OK_UTILITY)
		elog(ERROR, "Failed to drop transient table (%d)", spi_res);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/*
	 * Ensure invalidation of the relation of caches of all other backends,
	 * some possibly waiting for the relation exclusive lock. Invalidations
	 * include closing of the old node at SMGR level, so that the now waiting
	 * backends don't try to use the old node(s).
	 */
	CommandCounterIncrement();

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
setup_decoding(void)
{
	LogicalDecodingContext *ctx;
	MemoryContext oldcontext;

	Assert(!MyReplicationSlot);

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
	MemoryContextSwitchTo(oldcontext);
	return ctx;
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
	StdRdOptions *options;
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
	 * but it's not critical. If someone managed to change attribute or index
	 * concurrently (shouldn't happen because AccessShareLock should be held
	 * on the source relation now), new snapshot could see the change(s), but
	 * snapshot builder should also include them into the first historic
	 * snapshot (by waiting for the writing transaction(s) to commit). The
	 * point is that only DDLs on the top of the first historic snapshot are
	 * worth attention.
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
	options = (StdRdOptions *) extractRelOptions(tuple, desc, NULL);
	if (options == NULL || !options->user_catalog_table)
	{
		Assert(!result->is_catalog);
		return result;
	}

	result->relid = relid;
	result->is_catalog = true;
	result->relnatts = form_class->relnatts;

	/*
	 * pg_class(xmin) helps to ensure that the "user_catalog_option" wasn't
	 * turned off and on. On the other hand it might restrict some concurrent
	 * DDLs that would be safe as such.
	 */
	result->pg_class_xmin = HeapTupleHeaderGetXmin(tuple->t_data);

	if (form_class->relhasindex)
		result->indexes = get_index_info(relid, &result->relninds,
										 &result->invalid_index, false,
										 snapshot);

	/* If any index is "invalid", no more catalog information is needed. */
	if (result->invalid_index)
		return result;

	if (result->relnatts > 0)
		result->attr_xmins = get_attribute_xmins(relid, result->relnatts,
												 snapshot);

	/* Cleanup. */
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
	pfree(desc);

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

		result[i] = HeapTupleHeaderGetXmin(tuple->t_data);
		n++;
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
		if (i == n)
		{
			/* Index added concurrently? */
			mismatch = true;
			break;
		}
		result[i++].pg_class_xmin = HeapTupleHeaderGetXmin(tuple->t_data);
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
static void
check_catalog_changes(CatalogState *state, LOCKMODE lock_held)
{
	/*
	 * No DDL should be compatible with this lock mode. (Not sure if this
	 * condition will ever fire.)
	 */
	if (lock_held == AccessExclusiveLock)
		return;

	/*
	 * Only AccessExclusiveLock guarantees that the pg_class entry hasn't
	 * changed. By lowering this threshold we'd perhaps skip unnecessary check
	 * sometimes (e.g. change of pg_class(relhastriggers) is unimportant), but
	 * we could also miss the check when necessary. It's simply too fragile to
	 * deduce the kind of DDL from lock level, so do this check
	 * unconditionally.
	 */
	check_pg_class_changes(state->relid, state->pg_class_xmin, lock_held);

	/*
	 * Attribute and index changes should really need AccessExclusiveLock, so
	 * also call them unconditionally.
	 */
	check_index_changes(state->relid, state->indexes, state->relninds);
	check_attribute_changes(state->relid, state->attr_xmins, state->relnatts);
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
	if (state->attr_xmins)
		pfree(state->attr_xmins);
	if (state->indexes)
		pfree(state->indexes);
	pfree(state);
}

/*
 * Create column list out of a set of catalog tuples, to be used in CREATE
 * TABLE command.
 */
static char *
get_column_list(SPITupleTable *cat_data)
{
	TupleDesc	tupdesc;
	int	i, j;
	StringInfo	result, colname_buf = NULL;

	result = makeStringInfo();
	tupdesc = SPI_tuptable->tupdesc;
	j = 1;
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup;
		const char	*colname;
		const char	*typname;
		bool	valisnull;
		Datum	isdropped;

		tup = SPI_tuptable->vals[i];

		isdropped = SPI_getbinval(tup, tupdesc, 2, &valisnull);
		Assert(!valisnull);

		if (!isdropped)
		{
			colname = quote_identifier(SPI_getvalue(tup, tupdesc, 1));
			typname = SPI_getvalue(tup, tupdesc, 3);
		}
		else
		{
			/*
			 * Dropped column must be preserved as a placeholder, so that
			 * pg_attribute works for the new table.
			 */
			if (colname_buf == NULL)
				/* First time through. */
				colname_buf = makeStringInfo();
			else
				resetStringInfo(colname_buf);
			appendStringInfo(colname_buf, "dropped_%d", j++);
			colname = colname_buf->data;

			/*
			 * pg_attribute(atttypid) is no longer available, so use arbitrary
			 * type. We'll only insert NULL values into the column.
			 */
			typname = "bool";
		}

		if (i > 0)
			appendStringInfoString(result, ", ");

		appendStringInfo(result, "%s %s", colname, typname);
	}

	return result->data;
}


/*
 * Install the passed historic snapshot or uninstall it if NULL is passed.
 *
 * TODO
 *
 * 1. Consider if we'd better invalidate the whole catalog cache. (If so, use
 * InvalidateSystemCaches()?)
 *
 * 2. If the unused argument of TeardownHistoricSnapshot() is clarified, pass
 * it to this function too.
 *
 * 3. Make sure the historic snapshot is uninstalled on ERROR anywhere.
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
		 * The SPI uses GetActiveSnapshot() for read-only transactions and
		 * GetTransactionSnapshot() for writing ones. Adjust both to see
		 * consistent behavior.
		 */
		PushActiveSnapshot(snap_hist);

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
		Assert(HistoricSnapshotActive());
		TeardownHistoricSnapshot(false);
		/* Revert the hack done above. */
		FirstSnapshotSet = true;

		PopActiveSnapshot();
	}
}

/*
 * Use snap_hist snapshot to get the relevant data from rel_src and insert it
 * into rel_dst.
 *
 * Caller is responsible for opening and locking both relations.
 */
static void
perform_initial_load(Relation rel_src, Oid cluster_idx_id, Snapshot snap_hist,
					 Relation rel_dst)
{
	bool	use_sort;
	int	batch_size, batch_max_size;
	Tuplesortstate *tuplesort = NULL;
	Relation	cluster_idx = NULL;
	HeapScanDesc	heap_scan = NULL;
	IndexScanDesc	index_scan = NULL;
	HeapTuple	*tuples = NULL;
	ResourceOwner	res_owner_old, res_owner_plan;

	/*
	 * The initial load starts by fetching data from the source table and
	 * should not include concurrent changes.
	 */
	switch_snapshot(snap_hist);

	if (OidIsValid(cluster_idx_id))
	{
		cluster_idx = relation_open(cluster_idx_id, AccessShareLock);
		Assert(cluster_idx->rd_rel->relam == BTREE_AM_OID);

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
		use_sort = plan_cluster_use_sort(rel_src->rd_id, cluster_idx_id);

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

	if (use_sort)
	{
		heap_scan = heap_beginscan(rel_src, snap_hist, 0, (ScanKey) NULL);

		tuplesort = tuplesort_begin_cluster(RelationGetDescr(rel_src),
											cluster_idx, maintenance_work_mem,
											false);
	}
	else
	{
		index_scan = index_beginscan(rel_src, cluster_idx, snap_hist, 0, 0);
		index_rescan(index_scan, NULL, 0, NULL, 0);
	}

	/*
	 * TODO Unless tuplesort is used, tune the batch_max_size and consider
	 * automatic adjustment, so that maintenance_work_mem is not
	 * exceeded. (The current low value is there for development purposes
	 * only.)
	 */
	batch_max_size = 2;
	if (!use_sort)
		tuples = (HeapTuple *) palloc0(batch_max_size * sizeof(HeapTuple));
	while (true)
	{
		HeapTuple	tup_in;
		int	i = 0;

		/* Sorting cannot be split into batches. */
		for (; use_sort || i < batch_max_size; i++)
		{
			tup_in = use_sort ?
				heap_getnext(heap_scan, ForwardScanDirection) :
				index_getnext(index_scan, ForwardScanDirection);
			if (tup_in != NULL)
			{
				if (use_sort)
					tuplesort_putheaptuple(tuplesort, tup_in);
				else
					tuples[i] = heap_copytuple(tup_in);
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


			/* TODO Enable bulk insert. */
			heap_insert(rel_dst, tup_out, GetCurrentCommandId(true), 0, NULL);
			if (!use_sort && should_free)
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

static void
decode_concurrent_changes(LogicalDecodingContext *ctx, XLogRecPtr *startptr,
	XLogRecPtr end_of_wal)
{
	/*
	 * TODO Double-check if the PG_TRY section does not have to start
	 * earlier. (Probably not only decoding should affect the catalog cache
	 * and there should have been no real decoding during slot
	 * initialization.)
	 */
	PG_TRY();
	{
		while ((*startptr != InvalidXLogRecPtr && *startptr < end_of_wal) ||
			   (ctx->reader->EndRecPtr != InvalidXLogRecPtr &&
				ctx->reader->EndRecPtr < end_of_wal))
		{
			XLogRecord *record;
			char	   *errm = NULL;

			record = XLogReadRecord(ctx->reader, *startptr, &errm);
			if (errm)
				elog(ERROR, "%s", errm);

			*startptr = InvalidXLogRecPtr;

			if (record != NULL)
				LogicalDecodingProcessRecord(ctx, ctx->reader);

			CHECK_FOR_INTERRUPTS();
		}
		InvalidateSystemCaches();
	}
	PG_CATCH();
	{
		InvalidateSystemCaches();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Process changes that happened during the initial load.
 *
 * Scan key is passed by caller, so it does not have to be constructed
 * multiple times.
 *
 * Index list is passed explicitly as the relation cache entry is not supposed
 * to reflect changes of our transaction (unless we want to reload it, which
 * seems an overkill).
 *
 * For the same reason, ident_index is passed separate. (XXX The PoC does not
 * create constraints, so relation->rd_replidindex field would be empty
 * anyway. But this approach might change in the future.)
 */
static void
process_concurrent_changes(DecodingOutputState *s, Relation relation,
						   ScanKey key, int nkeys, Oid *indexes, int nindexes,
						   Oid ident_index)
{
	TupleTableSlot	*slot_data, *slot_metadata;
	HeapTuple tup_old = NULL;

	slot_metadata = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot_metadata, s->metadata.tupdesc);

	slot_data = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot_data, s->data.tupdesc);

	while (tuplestore_gettupleslot(s->metadata.tupstore, true, false,
								   slot_metadata))
	{
		HeapTuple tup_meta, tup, tup_exist;
		Datum	kind_value[1];
		bool	kind_isnull[1];
		char	change_kind;

		if (!tuplestore_gettupleslot(s->data.tupstore, true, false,
									 slot_data))
			elog(ERROR, "The data and metadata slots do not match.");

		tup_meta = ExecCopySlotTuple(slot_metadata);
		heap_deform_tuple(tup_meta, slot_metadata->tts_tupleDescriptor,
						  kind_value, kind_isnull);
		Assert(!kind_isnull[0]);
		change_kind = DatumGetChar(kind_value[0]);

		tup = ExecCopySlotTuple(slot_data);
		if (change_kind == PG_SQUEEZE_CHANGE_UPDATE_OLD)
		{
			Assert(tup_old == NULL);
			tup_old = tup;
		}
		else if (change_kind == PG_SQUEEZE_CHANGE_INSERT)
		{
			Assert(tup_old == NULL);
			/* TODO Bulk insert if several consecutive changes are
			 * INSERT. (But impose limit on memory.) */
			heap_insert(relation, tup, GetCurrentCommandId(true), 0, NULL);
			update_indexes(relation, tup, indexes, nindexes);
			pfree(tup);
		}
		else if (change_kind == PG_SQUEEZE_CHANGE_UPDATE_NEW ||
				 change_kind == PG_SQUEEZE_CHANGE_DELETE)
		{
			HeapTuple	tup_key;
			IndexScanDesc	scan;
			int i;
			ItemPointerData	ctid;
			Relation	index;

			if (change_kind == PG_SQUEEZE_CHANGE_UPDATE_NEW)
			{
				tup_key = tup_old != NULL ? tup_old : tup;
			}
			else
			{
				Assert(tup_old == NULL);
				tup_key = tup;
			}

			/* No lock, the parent relation is not yet visible to others. */
			index = index_open(ident_index, NoLock);

			/*
			 * Find the tuple to be updated or deleted.
			 *
			 * XXX Not sure we need PushActiveSnapshot() - as the table is not
			 * visible to other transactions, the xmin, xmax, xip, etc. fields
			 * of the snapshot are not important, and CurrentSnapshot->curcid
			 * should stay consistent with CommandCounterIncrement() even if
			 * GetSnapshotData() gets called anytime.
			 *
			 * XXX As no other transactions are engaged, SnapshotSelf might
			 * seem to prevent us from wasting values of the command counter
			 * (as we do not update catalog here, cache invalidation is not
			 * the reason to increment the counter). However, heap_update()
			 * does require CommandCounterIncrement().
			 */
			scan = index_beginscan(relation, index, GetTransactionSnapshot(),
								   nkeys, 0);

			index_rescan(scan, key, nkeys, NULL, 0);

			/* Use the incoming tuple to finalize the scan key. */
			for (i = 0; i < scan->numberOfKeys; i++)
			{
				ScanKey	entry;
				bool	isnull;

				entry = &scan->keyData[i];
				entry->sk_argument = heap_getattr(tup_key,
												  entry->sk_attno,
												  relation->rd_att,
												  &isnull);
				Assert(!isnull);
			}
			tup_exist = index_getnext(scan, ForwardScanDirection);
			if (tup_exist == NULL)
				elog(ERROR, "Failed to find target tuple");
			ItemPointerCopy(&tup_exist->t_self, &ctid);
			index_endscan(scan);
			index_close(index, NoLock);

			if (change_kind == PG_SQUEEZE_CHANGE_UPDATE_NEW)
			{
				simple_heap_update(relation, &ctid, tup);
				if (!HeapTupleIsHeapOnly(tup))
					update_indexes(relation, tup, indexes, nindexes);
			}
			else
				simple_heap_delete(relation, &ctid);

			if (tup_old != NULL)
			{
				pfree(tup_old);
				tup_old = NULL;
			}

			pfree(tup);
		}
		else
			elog(ERROR, "Unrecognized kind of change: %d", change_kind);

		/* If there's any change, make it visible to the next iteration. */
		if (change_kind != PG_SQUEEZE_CHANGE_UPDATE_OLD)
			CommandCounterIncrement();
		pfree(tup_meta);
	}

	if (tuplestore_gettupleslot(s->data.tupstore, true, false,
								 slot_data))
		elog(ERROR, "The data and metadata slots do not match.");

	ExecDropSingleTupleTableSlot(slot_data);
	ExecDropSingleTupleTableSlot(slot_metadata);
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
						Oid *indexes_src, int nindexes)
{
	StringInfo	ind_name;
	int	i;
	Oid	*result;

	Assert(nindexes > 0);

	ind_name = makeStringInfo();
	result = (Oid *) palloc(nindexes * sizeof(Oid));

	for (i = 0; i < nindexes; i++)
	{
		Oid	ind_oid, ind_oid_new;
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
			if (heap_col_id >= 1)
			{
				Form_pg_attribute	att;

				/* Normal attribute. */
				att = rel_src->rd_att->attrs[heap_col_id - 1];
				colname = NameStr(att->attname);
				collations[j] = att->attcollation;
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
				colname = col_name_buf->data;
				collations[j] = InvalidOid;
			}
			else
				elog(ERROR, "Unexpected column number: %d",
					 heap_col_id);

			colnames = lappend(colnames, pstrdup(colname));
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
								   rel_dst->rd_rel->reltablespace,
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


/* Insert ctid into all indexes of relation. */
static void
update_indexes(Relation heap, HeapTuple tuple, Oid *indexes, int nindexes)
{
	int i;
	Datum	*values_heap;
	bool	*isnull_heap;
	int	natts_heap;
	ItemPointerData	ctid;

	/*
	 * TODO Find out if FormIndexDatum() works with MinimalTuple. Repeated
	 * retrieval from slot might not require deforming of the whole heap
	 * tuple.
	 */
	natts_heap = heap->rd_att->natts;
	values_heap = (Datum *) palloc(natts_heap * sizeof(Datum));
	isnull_heap = (bool *) palloc(natts_heap * sizeof(bool));
	heap_deform_tuple(tuple, heap->rd_att, values_heap, isnull_heap);
	ItemPointerCopy(&tuple->t_self, &ctid);

	for (i = 0; i < nindexes; i++)
	{
		Relation rel_ind;
		Datum *values;
		bool *isnull;
		Form_pg_index	ind_form;
		int	indnatts;
		int	j;

		rel_ind = index_open(indexes[i], RowExclusiveLock);
		ind_form = rel_ind->rd_index;
		indnatts = ind_form->indnatts;
		values = (Datum *) palloc(indnatts * sizeof(Datum));
		isnull = (bool *) palloc(indnatts * sizeof(bool));
		for (j = 0; j < indnatts; j++)
		{
			AttrNumber	attno_heap;

			attno_heap = ind_form->indkey.values[j] - 1;
			values[j] = values_heap[attno_heap];
			isnull[j] = isnull_heap[attno_heap];
		}
		index_insert(rel_ind, values, isnull, &ctid, heap,
					 ind_form->indisunique);
		pfree(values);
		pfree(isnull);
		index_close(rel_ind, RowExclusiveLock);
	}
	pfree(values_heap);
	pfree(isnull_heap);
}

/*
 * Derived from swap_relation_files() in PG core, but removed anything we
 * don't need. Also incorporated the relevant parts of finish_heap_swap().
 *
 * Important: r1 is the relation to remain, r2 is the one to be dropped.
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
	char		swptmpchr;
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

		swptmpchr = relform1->relpersistence;
		relform1->relpersistence = relform2->relpersistence;
		relform2->relpersistence = swptmpchr;

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
