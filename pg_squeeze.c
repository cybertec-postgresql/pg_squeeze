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
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
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

	/*
	 * xmin of the pg_class tuple of the source relation during the initial
	 * check.
	 */
	TransactionId		pg_class_xmin;
} CatalogState;

static LogicalDecodingContext *setup_decoding(void);
static CatalogState *get_catalog_state(Oid relid);
static void check_catalog_changes(CatalogState *state);
static char *get_column_list(SPITupleTable *cat_data, bool create);
static void switch_snapshot(Snapshot snap_hist);
static void decode_concurrent_changes(LogicalDecodingContext *ctx,
									  XLogRecPtr *startptr,
									  XLogRecPtr end_of_wal);
static void process_concurrent_changes(DecodingOutputState *s,
									   Relation relation, ScanKey key,
									   int nkeys, Oid *indexes, int nindexes,
									   Oid ident_index);
static void build_transient_indexes(Relation rel_dst, Relation rel_src,
									Oid **indexes_src, Oid **indexes_dst,
									int *nindexes);
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
	Relation	rel_src, rel_dst, ident_idx_rel;
	Form_pg_index ident_idx;
	Oid	ident_idx_id, ident_idx_dst;
	Oid	relid_src, relid_dst;
	char	replident;
	ScanKey	key;
	int	i, nkeys;
	LogicalDecodingContext	*ctx;
	Snapshot	snap_hist;
	StringInfo	relname_tmp, stmt;
	char	*relname_src, *relname_dst, *create_stmt;
	int	spi_res;
	SPIPlanPtr	plan;
	Portal	portal;
	TupleTableSlot	*slot;
	Tuplestorestate	*tsstate;
	TupleDesc	tup_desc;
	CatalogState		*cat_state;
	DecodingOutputState	*dstate;
	XLogRecPtr	end_of_wal, startptr;
	ResourceOwner resowner_old, resowner_decode;
	XLogRecPtr	xlog_insert_ptr;
	int	nindexes;
	Oid	*indexes_src, *indexes_dst;

	relname = PG_GETARG_TEXT_P(0);
	relrv_src = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	/* TODO Consider heap_open() / heap_close(). */
	rel_src = relation_openrv(relrv_src, AccessShareLock);

	RelationGetIndexList(rel_src);
	replident = rel_src->rd_rel->relreplident;

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
		(replident == REPLICA_IDENTITY_DEFAULT &&
		 !OidIsValid(rel_src->rd_replidindex)))
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Table is not selective"))));

	/* Change processing w/o index is not a good idea. */
	if (replident == REPLICA_IDENTITY_FULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Replica identity \"full\" not supported"))));

	/* Build scan key to process logical changes. */
	/* TODO Consider constructing the key later, when the destination table
	 * also has it (so it's constructed short before use). (Also move this
	 * part into a separate function.) */
	Assert(OidIsValid(rel_src->rd_replidindex));
	ident_idx_rel = index_open(rel_src->rd_replidindex, AccessShareLock);
	ident_idx = ident_idx_rel->rd_index;
	ident_idx_id = ident_idx_rel->rd_id;
	nkeys = ident_idx->indnatts;
	key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	for (i = 0; i < nkeys; i++)
	{
		ScanKey	entry;
		int16	relattno;
		TupleDesc	desc;
		Form_pg_attribute	att;
		Oid	opfamily, opno, opcode;

		entry = &key[i];
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
	if (!cat_state->is_catalog)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("\"user_catalog_table\" option not set"))));

	/*
	 * Since the final check follows initialization of the replication slot,
	 * the relation shouldn't be locked between the checks. Otherwise another
	 * transaction could write XLOG records before the slots' data.restart_lsn
	 * and we'd have to wait for it to finish. If such a transaction requested
	 * exclusive lock on our relation (e.g. ALTER TABLE), it'd result in a
	 * deadlock.
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

	/* TODO Include constraints (including FK). */
	stmt = makeStringInfo();
	appendStringInfo(stmt,
					 "SELECT a.attname, a.attnotnull, "
					 "  pg_catalog.format_type(a.atttypid, NULL)"
					 "FROM pg_catalog.pg_attribute a "
					 "WHERE a.attrelid = %u AND a.attnum > 0 AND "
					 "NOT a.attisdropped", relid_src);

	if ((spi_res = SPI_exec(stmt->data, 0)) != SPI_OK_SELECT)
		elog(ERROR, "Failed to get definition of table %s (%d)",
			 relname_dst, spi_res);

	/* Return to presence so far. */
	switch_snapshot(NULL);

	/*
	 * If user is adding columns concurrently, it'd cause ERROR during the
	 * subsequent decoding anyway.
	 */
	if (SPI_processed < 1)
		/* XXX Try to find better error code. */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 (errmsg("Table %s has no columns", relname_src))));

	/* Construct CREATE TABLE command. */
	/*
	 * TODO The command contains NOT NULL constraints. Add PK, UNIQUE and FK
	 * ones when the data has been loaded. (This means that we need to extract
	 * the relevant data from the tuple and store it in local memory table
	 * before we use SPI to load the data. Better option: create the
	 * constraint strings right away and use them later?)
	 */
	resetStringInfo(stmt);
	appendStringInfo(stmt, "CREATE TABLE %s ", relname_dst);
	appendStringInfo(stmt, "(%s)", get_column_list(SPI_tuptable, true));
	create_stmt = pstrdup(stmt->data);

	/*
	 * Use the same SPI_tuptable to construct the query for the initial load.
	 */
	resetStringInfo(stmt);
	appendStringInfo(stmt, "SELECT %s ",
					 get_column_list(SPI_tuptable, false));
	appendStringInfo(stmt, "FROM ONLY %s", relname_src);
	/* TODO Add ORDER BY clause, to ensure clustering. */

	if ((spi_res = SPI_exec(create_stmt, 0)) != SPI_OK_UTILITY)
		elog(ERROR, "Failed to create temporary table %s (%d)",
			 relname_dst, spi_res);

	/*
	 * Any activity concerning the initial load needs the historic
	 * snapshot. Thus we ignore data changes another transaction might be
	 * doing right now - we'll use logical decoding to retrieve them later.
	 *
	 * Note that we only retrieve the data now and put it into a temporary
	 * storage. Insertion while using the historic snapshot (INSERT INTO
	 * .. SELECT ...) probably isn't a good idea.
	 */
	switch_snapshot(snap_hist);
	plan = SPI_prepare(stmt->data, 0, NULL);
	if (plan == NULL)
		elog(ERROR, "Failed to prepare plan");
	switch_snapshot(NULL);

	/*
	 * Now that the plan exists, the source table is locked. We need at least
	 * to know whether the catalog option was never changed, but the other
	 * checks are important too.
	 *
	 * XXX If replacing SPI with low level code (e.g. "relation rewrite") and
	 * if this call of check_catalog_changes() appears to be the last one,
	 * make sure the (share) lock on the source relation is also kept till the
	 * end of the transaction). Otherwise we'd ought to call
	 * check_catalog_changes() before any set of changes is applied below, and
	 * preferrably before any costly operation (e.g. index creation) is
	 * started. That brings special deadlock concern (catalog tables vs. user
	 * tables lock order) when calling check_catalog_changes() after having
	 * acquired exclusive lock on the source table.
	 *
	 * TODO Investigate if next call of this function is needed. The ALTER
	 * statements affecting pg_class should be blocked now because of the
	 * SELECT plan. Is there any other kind of DDL that breaks our work, but
	 * does not change pg_class? For example, a transaction that adds one
	 * column and drops another one does not change pg_class(relnatts). Does
	 * it still change pg_class(xmin)? And how about FK, CHECK or UNIQUE?
	 */
	check_catalog_changes(cat_state);

	/*
	 * TODO Check in similar way that there's no change in pg_attribute. That
	 * change includes renaming check_catalog_option to check_catalog_changes.
	 */

	/*
	 * No lock is needed on the target relation - no other transaction should
	 * be able to see it yet.
	 */
	relrv_dst = makeRangeVar(relrv_src->schemaname, relname_tmp->data, -1);
	rel_dst = relation_openrv(relrv_dst, NoLock);
	relid_dst = rel_dst->rd_id;

	/*
	 * The initial load starts by fetching data from the source table and
	 * should not include concurrent changes.
	 */
	switch_snapshot(snap_hist);

	/*
	 * TODO Tune the in-memory size. That may include dynamic adjustments of
	 * the limit of SPI_cursor_fetch().
	 */
	slot = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot, tup_desc);
	tsstate = tuplestore_begin_heap(false, false, work_mem);
	portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
	while (true)
	{
		int	i;

		SPI_cursor_fetch(portal, true, 1024);

		/* Done? */
		if (SPI_processed == 0)
			break;

		for (i = 0; i < SPI_processed; i++)
		{
			tuplestore_puttuple(tsstate, SPI_tuptable->vals[i]);
		}

		/*
		 * TODO consider additional fetch if the tuplestore seems to be have
		 * enough memory for the next batch.
		 */

		/* Insert the tuples into the target table. */
		switch_snapshot(NULL);
		while (tuplestore_gettupleslot(tsstate, true, false, slot))
		{
			HeapTuple tup;

			tup = ExecCopySlotTuple(slot);
			/* TODO Enable bulk insert. */
			heap_insert(rel_dst, tup, GetCurrentCommandId(true), 0, NULL);
			pfree(tup);
		}
		tuplestore_clear(tsstate);
		switch_snapshot(snap_hist);
	}
	SPI_cursor_close(portal);
	tuplestore_end(tsstate);
	ExecDropSingleTupleTableSlot(slot);

	/* The historic snapshot is no longer needed. */
	switch_snapshot(NULL);

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
	 * Check if concurrent DDL (affecting pg_class, pg_attribute or
	 * constraints of the soure table) took place concurrently. Only tuple
	 * descriptor change makes the decoded data unusable, but changes of
	 * constraint would introduce another class of problems, so reject them
	 * too.
	 *
	 * Indexes can take quite some effort to build and we don't want to waste
	 * it.
	 */
	check_catalog_changes(cat_state);

	/*
	 * Create indexes on the temporary table - that might take a
	 * while. (Unlike the concurrent changes, which we insert into existing
	 * indexes.)
	 */
	rel_src = relation_open(relid_src, AccessShareLock);
	build_transient_indexes(rel_dst, rel_src, &indexes_src, &indexes_dst,
							&nindexes);

	relation_close(rel_src, AccessShareLock);

	/* Find "identity index" of the transient relation. */
	ident_idx_dst = InvalidOid;
	for (i = 0; i < nindexes; i++)
	{
		if (ident_idx_id == indexes_src[i])
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
	 * Check for catalog changes again, to make sure no index changed.
	 *
	 * The point is to ensure that the changes decoded so far are compatible
	 * with the transient table (In fact, catalog change could have happened
	 * after the decoding had completed, but that breaks the whole procedure
	 * anyway. We'll check when needed again.)
	 */
	check_catalog_changes(cat_state);

	/* Process the first batch of concurrent changes. */
	process_concurrent_changes(dstate, rel_dst, key, nkeys, indexes_dst,
							   nindexes, ident_idx_dst);
	tuplestore_clear(dstate->data.tupstore);
	tuplestore_clear(dstate->metadata.tupstore);

	/*
	 * Lock the source table exclusively last time, to finalize the work.
	 *
	 * Note: To ensure that the invalidation messages (see the comment on
	 * CommandCounterIncrement below) are delivered safely before any other
	 * backend can acquire this lock after us, we never unlock the relation
	 * explicitly. Instead, xact.c will do so when committing our transaction.
	 */
	/*
	 * TODO As the index build could have taken long time, consider flushing
	 * and decoding (or even catalog check and processing?) as much XLOG as we
	 * can, so that less work is left to the exclusive lock time.
	 */
	LockRelationOid(relid_src, AccessExclusiveLock);

	/*
	 * Check the source relation for DDLs once again. If this check passes,
	 * no DDL can break the process anymore.
	 */
	/*
	 * TODO Consider if this special case requires locking the relevant
	 * catalog tables in front of the LockRelationOid() above and moving the
	 * lock / unlock code into separate functions. If lock on catalog relation
	 * is kept while accessing user relation, deadlock can occur.
	 */
	check_catalog_changes(cat_state);
	/* This was the last check. */
	pfree(cat_state);

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
	process_concurrent_changes(dstate, rel_dst, key, nkeys, indexes_dst,
							   nindexes, ident_idx_dst);

	pfree(key);
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
	HeapTuple	pg_class_tuple = NULL;
	Relation	pg_class_rel;
	TupleDesc	pg_class_desc;
	SysScanDesc pg_class_scan;
	ScanKeyData key[1];
	Snapshot	snapshot;
	StdRdOptions *options;
	CatalogState		*result;

	/*
	 * ScanPgRelation.c would do most of the work below, but relcache.c does
	 * not export it.
	 */
	ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));
	pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
	pg_class_desc = CreateTupleDescCopy(RelationGetDescr(pg_class_rel));

	/*
	 * The relation is not a real catalog relation, but GetCatalogSnapshot()
	 * does exactly what we need, i.e. retrieves the most recent snapshot. If
	 * changing this, make sure that isolation level has no impact on the
	 * snapshot "freshness".
	 */
	snapshot = GetCatalogSnapshot(relid);

	pg_class_scan = systable_beginscan(pg_class_rel, ClassOidIndexId,
									   true, snapshot, 1, key);
	pg_class_tuple = systable_getnext(pg_class_scan);

	/*
	 * The relation should be locked by caller, so it must not have
	 * disappeared.
	 */
	Assert(HeapTupleIsValid(pg_class_tuple));

	result = (CatalogState *) palloc0(sizeof(CatalogState));
	result->relid = relid;

	/* The "user_catalog_option" is essential. */
	options = (StdRdOptions *) extractRelOptions(pg_class_tuple,
												 pg_class_desc, NULL);
	if (options != NULL && options->user_catalog_table)
		result->is_catalog = true;

	/* Many DDLs do affect pg_class_xmin. */
	result->pg_class_xmin = HeapTupleHeaderGetXmin(pg_class_tuple->t_data);

	/* Cleanup. */
	systable_endscan(pg_class_scan);
	heap_close(pg_class_rel, AccessShareLock);
	pfree(pg_class_desc);
	return result;
}


/*
 * Compare the passed catalog information to the info retrieved using the most
 * recent catalog snapshot. Perform the cheapest checks first, the trickier
 * ones later.
 *
 * Note that it makes no sense to check state->is_catalog here. Even true
 * value does not tell whether "user_catalog_option" was never changed back
 * and forth. pg_class(xmin) will reveal any change of the storage option.
 *
 * (As long as we use xmin columns of the catalog tables to detect changes, we
 * can't use syscache here.)
 *
 * Unlike get_catalog_state(), this function requires no lock on the relation
 * being checked, except for the last check. (If the absence of a lock allows
 * for concurrent changes, the next call will reveal those.)
 */
static void
check_catalog_changes(CatalogState *state)
{
	HeapTuple	pg_class_tuple = NULL;
	Relation	pg_class_rel;
	TupleDesc	pg_class_desc;
	SysScanDesc pg_class_scan;
	ScanKeyData key[1];
	Snapshot	snapshot;
	TransactionId		pg_class_xmin;

	/* This part is identical to the beginning of get_catalog_state(). */
	ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(state->relid));
	pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
	pg_class_desc = CreateTupleDescCopy(RelationGetDescr(pg_class_rel));
	snapshot = GetCatalogSnapshot(state->relid);
	pg_class_scan = systable_beginscan(pg_class_rel, ClassOidIndexId,
									   true, snapshot, 1, key);
	pg_class_tuple = systable_getnext(pg_class_scan);

	/* As the relation might not be locked, it could have disappeared. */
	if (!HeapTupleIsValid(pg_class_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 (errmsg("Table no longer exists"))));

	/* Check if pg_class(xmin) has changed. */
	pg_class_xmin = HeapTupleHeaderGetXmin(pg_class_tuple->t_data);
	if (!TransactionIdEquals(pg_class_xmin, state->pg_class_xmin))
		/* XXX Does more suitable error code exist? */
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("Concurrent DDL detected")));

	systable_endscan(pg_class_scan);
	heap_close(pg_class_rel, AccessShareLock);
	pfree(pg_class_desc);
}

/*
 * Create column list out of a set of catalog tuples. If "create" is true, the
 * list will be used for CREATE TABLE command, otherwise it's a target list of
 * SELECT.
 */
static char *
get_column_list(SPITupleTable *cat_data, bool create)
{
	TupleDesc	tupdesc;
	int	i;
	StringInfo	result;

	result = makeStringInfo();
	tupdesc = SPI_tuptable->tupdesc;
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup;
		const char	*colname;

		tup = SPI_tuptable->vals[i];
		colname = quote_identifier(SPI_getvalue(tup, tupdesc, 1));

		if (i > 0)
			appendStringInfoString(result, ", ");

		appendStringInfoString(result, colname);

		if (create)
		{
			const char	*typname;
			Datum	notnull;
			bool	valisnull;

			notnull = SPI_getbinval(tup, tupdesc, 2, &valisnull);
			Assert(!valisnull);
			typname = SPI_getvalue(tup, tupdesc, 3);

			appendStringInfo(result, " %s", typname);
			if (DatumGetBool(notnull))
				appendStringInfoString(result, " NOT NULL");
		}
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
/*
 * TODO Use the executor API - that takes care of constraints and indexes
 * (including index predicates), that we'll probably neeed to create after the
 * initial load. The question is how to generate a plan.
 *
 * If we take the "rewrite" approach, does the executor allow us to preserve
 * heap tuple header (xmin, xmax, flags etc.) of the original row?
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
			SnapshotData SnapshotDirty;

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
			 * TODO Find out what's different about SnapshotSelf or
			 * DirtySnapshot (the snapshot affects processing of multiple
			 * changes of the same row within the same transaction.)q
			 */
			InitDirtySnapshot(SnapshotDirty);
			scan = index_beginscan(relation, index, &SnapshotDirty, nkeys, 0);
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
 * nindexes receives the number of indexes processed. If it's positive,
 * indexes_src and indexes_dst receive oids of the source destination relation
 * indexes respectively. The order of items does match, so we can use these
 * arrays to swap index storage.
 */
static void
build_transient_indexes(Relation rel_dst, Relation rel_src,
						Oid **indexes_src, Oid **indexes_dst, int *nindexes)
{
	List	*ind_list_src;
	ListCell	*lc;
	StringInfo	ind_name;
	int	i;
	Oid	*result_src, *result_dst;

	ind_name = makeStringInfo();

	/*
	 * TODO Process rd_oidindex if it exists. (Simply by lappend() to
	 * ind_list_src?)
	 */
	ind_list_src = RelationGetIndexList(rel_src);

	Assert(nindexes != NULL);
	*nindexes = list_length(ind_list_src);
	if (*nindexes == 0)
		return;

	result_src = (Oid *) palloc(*nindexes * sizeof(Oid));
	result_dst = (Oid *) palloc(*nindexes * sizeof(Oid));

	i = 0;
	foreach (lc, ind_list_src)
	{
		Oid	ind_oid, ind_oid_new;
		Relation	ind;
		IndexInfo	*ind_info;
		int	j, heap_col_id;
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

		ind_oid = lfirst_oid(lc);
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
			Form_pg_attribute	att;

			heap_col_id = ind->rd_index->indkey.values[j] - 1;
			att = ind->rd_att->attrs[heap_col_id];
			colname = NameStr(att->attname);
			colnames = lappend(colnames, pstrdup(colname));
			collations[j] = att->attcollation;
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
		result_src[i] = ind_oid;
		result_dst[i] = ind_oid_new;

		list_free_deep(colnames);
		pfree(collations);
		pfree(opclasses);

		i++;
	}
	list_free(ind_list_src);

	Assert(indexes_src != NULL && indexes_dst != NULL);
	*indexes_src = result_src;
	*indexes_dst = result_dst;
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

			attno_heap = ind_form->indkey.values[j];
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

	/* set rel1's frozen Xid and minimum MultiXid */
	if (relform1->relkind != RELKIND_INDEX)
	{
		TransactionId frozenXid;
		MultiXactId cutoffMulti;

		frozenXid = relform2->relfrozenxid;
		Assert(TransactionIdIsNormal(frozenXid));
		relform1->relfrozenxid = frozenXid;

		cutoffMulti = relform2->relminmxid;
		Assert(MultiXactIdIsValid(cutoffMulti));
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
