/*-----------------------------------------------------------------------------------
 *
 * concurrent.c
 *	  Module to handle changes that took place while new table was being
 *	  created
 *
 * Copyright (c) 2016-2023, CYBERTEC PostgreSQL International GmbH
 *
 *-----------------------------------------------------------------------------------
 */


#include "pg_squeeze.h"

#if PG_VERSION_NUM >= 130000
#include "access/heaptoast.h"
#endif
#include "executor/executor.h"
#include "replication/decode.h"
#include "utils/rel.h"

#if PG_VERSION_NUM < 150000
extern PGDLLIMPORT int wal_segment_size;
#endif

static void apply_concurrent_changes(DecodingOutputState *dstate,
									 Relation relation, ScanKey key,
									 int nkeys, IndexInsertState *iistate,
									 struct timeval *must_complete);
static bool processing_time_elapsed(struct timeval *utmost);

static void plugin_startup(LogicalDecodingContext *ctx,
						   OutputPluginOptions *opt, bool is_init);
static void plugin_shutdown(LogicalDecodingContext *ctx);
static void plugin_begin_txn(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn);
static void plugin_commit_txn(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation rel, ReorderBufferChange *change);
static void store_change(LogicalDecodingContext *ctx,
						 ConcurrentChangeKind kind, HeapTuple tuple);
static HeapTuple get_changed_tuple(ConcurrentChange *change);
static bool plugin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id);

/*
 * Decode and apply concurrent changes. If there are too many of them, split
 * the processing into multiple iterations so that the intermediate storage
 * (tuplestore) is not likely to be written to disk.
 *
 * See check_catalog_changes() for explanation of lock_held argument.
 *
 * Returns true if must_complete is NULL or if managed to complete by the time
 * *must_complete indicates.
 */
bool
process_concurrent_changes(LogicalDecodingContext *ctx,
						   XLogRecPtr end_of_wal,
						   CatalogState *cat_state,
						   Relation rel_dst, ScanKey ident_key,
						   int ident_key_nentries, IndexInsertState *iistate,
						   LOCKMODE lock_held, struct timeval *must_complete)
{
	DecodingOutputState *dstate;
	bool		done;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/*
	 * If some changes could not be applied due to time constraint, make sure
	 * the tuplestore is empty before we insert new tuples into it.
	 */
	if (dstate->nchanges > 0)
		apply_concurrent_changes(dstate, rel_dst, ident_key,
								 ident_key_nentries, iistate, NULL);
	Assert(dstate->nchanges == 0);

	done = false;
	while (!done)
	{
		exit_if_requested();

		done = decode_concurrent_changes(ctx, end_of_wal, must_complete);

		if (processing_time_elapsed(must_complete))
			/* Caller is responsible for applying the changes. */
			return false;

		if (dstate->nchanges == 0)
			continue;

		/* Make sure the changes are still applicable. */
		check_catalog_changes(cat_state, lock_held);

		/*
		 * XXX Consider if it's possible to check *must_complete and stop
		 * processing partway through. Partial cleanup of the tuplestore seems
		 * non-trivial.
		 */
		apply_concurrent_changes(dstate, rel_dst, ident_key,
								 ident_key_nentries, iistate, must_complete);

		if (processing_time_elapsed(must_complete))
			/* Like above. */
			return false;
	}

	return true;
}

/*
 * Decode logical changes from the XLOG sequence up to end_of_wal.
 *
 * Returns true iff done (for now), i.e. no more changes below the end_of_wal
 * can be decoded.
 */
bool
decode_concurrent_changes(LogicalDecodingContext *ctx,
						  XLogRecPtr end_of_wal,
						  struct timeval *must_complete)
{
	DecodingOutputState *dstate;
	ResourceOwner resowner_old;
#if PG_VERSION_NUM < 130000
	/* Workaround for XLogBeginRead() in setup_decoding(). */
	static	bool	first_time = true;
	XLogRecPtr	startptr;
#endif

	/*
	 * Invalidate the "present" cache before moving to "(recent) history".
	 *
	 * Note: The cache entry of the transient relation is not affected
	 * (because it was created by the current transaction), but the tuple
	 * descriptor shouldn't change anyway (as opposed to index info, which we
	 * change at some point). Moreover, tuples of the transient relation
	 * should not actually be deconstructed: reorderbuffer.c records the
	 * tuples, but - as it never receives the corresponding commit record -
	 * does not examine them in detail.
	 */
	InvalidateSystemCaches();

	dstate = (DecodingOutputState *) ctx->output_writer_private;
	resowner_old = CurrentResourceOwner;
	CurrentResourceOwner = dstate->resowner;

	PG_TRY();
	{
		while (ctx->reader->EndRecPtr < end_of_wal)
		{
			XLogRecord *record;
			XLogSegNo	segno_new;
			char	   *errm = NULL;
			XLogRecPtr	end_lsn;

#if PG_VERSION_NUM < 130000
			if (first_time)
			{
				startptr = MyReplicationSlot->data.restart_lsn;
				first_time = false;
			}
			else
				startptr = InvalidXLogRecPtr;
#endif

			record = XLogReadRecord(ctx->reader,
#if PG_VERSION_NUM < 130000
									startptr,
#endif
									&errm);
			if (errm)
				elog(ERROR, "%s", errm);

			if (record != NULL)
				LogicalDecodingProcessRecord(ctx, ctx->reader);

			if (processing_time_elapsed(must_complete))
				break;

			/*
			 * If WAL segment boundary has been crossed, inform PG core that
			 * we no longer need the previous segment.
			 */
			end_lsn = ctx->reader->EndRecPtr;
			XLByteToSeg(end_lsn, segno_new, wal_segment_size);
			if (segno_new != squeeze_current_segment)
			{
				LogicalConfirmReceivedLocation(end_lsn);
				elog(DEBUG1, "pg_squeeze: confirmed receive location %X/%X",
					 (uint32) (end_lsn >> 32), (uint32) end_lsn);
				squeeze_current_segment = segno_new;
			}

			exit_if_requested();
		}
		InvalidateSystemCaches();
		CurrentResourceOwner = resowner_old;
	}
	PG_CATCH();
	{
		InvalidateSystemCaches();
		CurrentResourceOwner = resowner_old;
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(DEBUG1, "pg_squeeze: %.0f changes decoded but not applied yet",
		 dstate->nchanges);

	return ctx->reader->EndRecPtr >= end_of_wal;
}

/*
 * Apply changes that happened during the initial load.
 *
 * Scan key is passed by caller, so it does not have to be constructed
 * multiple times. Key entries have all fields initialized, except for
 * sk_argument.
 */
static void
apply_concurrent_changes(DecodingOutputState *dstate, Relation relation,
						 ScanKey key, int nkeys, IndexInsertState *iistate,
						 struct timeval *must_complete)
{
	TupleTableSlot *slot;
	TupleTableSlot *ind_slot;
	Form_pg_index ident_form;
	int2vector *ident_indkey;
	HeapTuple	tup_old = NULL;
	BulkInsertState bistate = NULL;

	if (dstate->nchanges == 0)
		return;

	/* Info needed to retrieve key values from heap tuple. */
	ident_form = iistate->ident_index->rd_index;
	ident_indkey = &ident_form->indkey;

	/* TupleTableSlot is needed to pass the tuple to ExecInsertIndexTuples(). */
	slot = MakeSingleTupleTableSlot(dstate->tupdesc, &TTSOpsHeapTuple);
	iistate->econtext->ecxt_scantuple = slot;

	/* A slot to fetch tuples from identity index. */
	ind_slot = table_slot_create(relation, NULL);

	/*
	 * In case functions in the index need the active snapshot and caller
	 * hasn't set one.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	while (tuplestore_gettupleslot(dstate->tstore, true, false,
								   dstate->tsslot))
	{
		bool		shouldFree;
		HeapTuple	tup_change,
					tup,
					tup_exist;
		char	   *change_raw;
		ConcurrentChange *change;
		bool		isnull[1];
		Datum		values[1];

		Assert(dstate->nchanges > 0);
		dstate->nchanges--;

		/* Get the change from the single-column tuple. */
		tup_change = ExecFetchSlotHeapTuple(dstate->tsslot, false, &shouldFree);
		heap_deform_tuple(tup_change, dstate->tupdesc_change, values, isnull);
		Assert(!isnull[0]);

		/* This is bytea, but char* is easier to work with. */
		change_raw = (char *) DatumGetByteaP(values[0]);

		change = (ConcurrentChange *) VARDATA(change_raw);

		/*
		 * Do not keep buffer pinned for insert if the current change is
		 * something else.
		 */
		if (change->kind != PG_SQUEEZE_CHANGE_INSERT && bistate != NULL)
		{
			FreeBulkInsertState(bistate);
			bistate = NULL;
		}

		tup = get_changed_tuple(change);

		if (change->kind == PG_SQUEEZE_CHANGE_UPDATE_OLD)
		{
			Assert(tup_old == NULL);
			tup_old = tup;
		}
		else if (change->kind == PG_SQUEEZE_CHANGE_INSERT)
		{
			List	   *recheck;

			Assert(tup_old == NULL);

			/*
			 * If the next change will also be INSERT, we'll try to use the
			 * same buffer.
			 */
			if (bistate == NULL)
				bistate = GetBulkInsertState();

			heap_insert(relation, tup, GetCurrentCommandId(true), 0, bistate);

			/* Update indexes. */
			ExecStoreHeapTuple(tup, slot, false);
			recheck = ExecInsertIndexTuples(
#if PG_VERSION_NUM >= 140000
											iistate->rri,
#endif
											slot,
											iistate->estate,
#if PG_VERSION_NUM >= 140000
											false,	/* update */
#endif
											false,	/* noDupErr */
											NULL,	/* specConflict */
											NIL /* arbiterIndexes */
#if PG_VERSION_NUM >= 160000
											,
											false	/* onlySummarizing */
#endif
				);


			/*
			 * If recheck is required, it must have been preformed on the
			 * source relation by now. (All the logical changes we process
			 * here are already committed.)
			 */
			list_free(recheck);
			pfree(tup);

			/* Update the progress information. */
			SpinLockAcquire(&MyWorkerSlot->mutex);
			MyWorkerSlot->progress.ins += 1;
			SpinLockRelease(&MyWorkerSlot->mutex);
		}
		else if (change->kind == PG_SQUEEZE_CHANGE_UPDATE_NEW ||
				 change->kind == PG_SQUEEZE_CHANGE_DELETE)
		{
			HeapTuple	tup_key;
			IndexScanDesc scan;
			int			i;
			ItemPointerData ctid;

			if (change->kind == PG_SQUEEZE_CHANGE_UPDATE_NEW)
			{
				tup_key = tup_old != NULL ? tup_old : tup;
			}
			else
			{
				Assert(tup_old == NULL);
				tup_key = tup;
			}

			/*
			 * Find the tuple to be updated or deleted.
			 *
			 * XXX As no other transactions are engaged, SnapshotSelf might
			 * seem to prevent us from wasting values of the command counter
			 * (as we do not update catalog here, cache invalidation is not
			 * the reason to increment the counter). However, heap_update()
			 * does require CommandCounterIncrement().
			 */
			scan = index_beginscan(relation, iistate->ident_index,
								   GetActiveSnapshot(), nkeys, 0);

			index_rescan(scan, key, nkeys, NULL, 0);

			/* Use the incoming tuple to finalize the scan key. */
			for (i = 0; i < scan->numberOfKeys; i++)
			{
				ScanKey		entry;
				bool		isnull;
				int16		attno_heap;

				entry = &scan->keyData[i];
				attno_heap = ident_indkey->values[i];
				entry->sk_argument = heap_getattr(tup_key,
												  attno_heap,
												  relation->rd_att,
												  &isnull);
				Assert(!isnull);
			}
			if (index_getnext_slot(scan, ForwardScanDirection, ind_slot))
			{
				bool		shouldFreeInd;

				tup_exist = ExecFetchSlotHeapTuple(ind_slot, false,
												   &shouldFreeInd);
				/* TTSOpsBufferHeapTuple has .get_heap_tuple != NULL. */
				Assert(!shouldFreeInd);
			}
			else
				tup_exist = NULL;

			if (tup_exist == NULL)
				elog(ERROR, "Failed to find target tuple");
			ItemPointerCopy(&tup_exist->t_self, &ctid);
			index_endscan(scan);

			if (change->kind == PG_SQUEEZE_CHANGE_UPDATE_NEW)
			{
#if PG_VERSION_NUM >= 160000
				TU_UpdateIndexes update_indexes;
#endif

				simple_heap_update(relation, &ctid, tup
#if PG_VERSION_NUM >= 160000
								   ,
								   &update_indexes
#endif
					);
				/*
				 * In PG < 16, change of any indexed attribute makes HOT
				 * impossible, Therefore HOT update implies that no index
				 * needs to be updated.
				 *
				 * In PG >= 16, if only attributes of "summarizing indexes"
				 * change, HOT update is still possible. Therefore HOT update
				 * might still require some indexes (in particular, the
				 * summarizing ones) to be updated.
				 */
#if PG_VERSION_NUM >= 160000
				if (update_indexes != TU_None)
#else
				if (!HeapTupleIsHeapOnly(tup))
#endif
				{
					List	   *recheck;

					ExecStoreHeapTuple(tup, slot, false);

					/*
					 * XXX Consider passing update=true, however it requires
					 * es_range_table to be initialized. Is it worth the
					 * complexity?
					 */
					recheck = ExecInsertIndexTuples(
#if PG_VERSION_NUM >= 140000
													iistate->rri,
#endif
													slot,
													iistate->estate,
#if PG_VERSION_NUM >= 140000
													false,	/* update */
#endif
													false,	/* noDupErr */
													NULL,	/* specConflict */
													NIL /* arbiterIndexes */
#if PG_VERSION_NUM >= 160000
													,
					/* onlySummarizing */
													(update_indexes == TU_Summarizing)
#endif
						);
					list_free(recheck);
				}

				/* Update the progress information. */
				SpinLockAcquire(&MyWorkerSlot->mutex);
				MyWorkerSlot->progress.upd += 1;
				SpinLockRelease(&MyWorkerSlot->mutex);
			}
			else
			{
				simple_heap_delete(relation, &ctid);

				/* Update the progress information. */
				SpinLockAcquire(&MyWorkerSlot->mutex);
				MyWorkerSlot->progress.del += 1;
				SpinLockRelease(&MyWorkerSlot->mutex);
			}

			if (tup_old != NULL)
			{
				pfree(tup_old);
				tup_old = NULL;
			}

			pfree(tup);
		}
		else
			elog(ERROR, "Unrecognized kind of change: %d", change->kind);

		/* If there's any change, make it visible to the next iteration. */
		if (change->kind != PG_SQUEEZE_CHANGE_UPDATE_OLD)
		{
			CommandCounterIncrement();
			UpdateActiveSnapshotCommandId();
		}

		/* TTSOpsMinimalTuple has .get_heap_tuple==NULL. */
		Assert(shouldFree);
		pfree(tup_change);

		/*
		 * If there is a limit on the time of completion, check it
		 * now. However, make sure the loop does not break if tup_old was set
		 * in the previous iteration. In such a case we could not resume the
		 * processing in the next call.
		 */
		if (must_complete && tup_old == NULL &&
			processing_time_elapsed(must_complete))
			/* The next call will process the remaining changes. */
			break;
	}

	/* If we could not apply all the changes, the next call will do. */
	if (dstate->nchanges == 0)
		tuplestore_clear(dstate->tstore);

	PopActiveSnapshot();

	/* Cleanup. */
	if (bistate != NULL)
		FreeBulkInsertState(bistate);
	ExecDropSingleTupleTableSlot(slot);
	ExecDropSingleTupleTableSlot(ind_slot);
}

static bool
processing_time_elapsed(struct timeval *utmost)
{
	struct timeval now;

	if (utmost == NULL)
		return false;

	gettimeofday(&now, NULL);

	if (now.tv_sec < utmost->tv_sec)
		return false;

	if (now.tv_sec > utmost->tv_sec)
		return true;

	return now.tv_usec >= utmost->tv_usec;
}

IndexInsertState *
get_index_insert_state(Relation relation, Oid ident_index_id)
{
	EState	   *estate;
	int			i;
	IndexInsertState *result;

	result = (IndexInsertState *) palloc0(sizeof(IndexInsertState));
	estate = CreateExecutorState();
	result->econtext = GetPerTupleExprContext(estate);

	result->rri = (ResultRelInfo *) palloc(sizeof(ResultRelInfo));
	InitResultRelInfo(result->rri, relation, 0, 0, 0);
	ExecOpenIndices(result->rri, false);

	/*
	 * Find the relcache entry of the identity index so that we spend no extra
	 * effort to open / close it.
	 */
	for (i = 0; i < result->rri->ri_NumIndices; i++)
	{
		Relation	ind_rel;

		ind_rel = result->rri->ri_IndexRelationDescs[i];
		if (ind_rel->rd_id == ident_index_id)
			result->ident_index = ind_rel;
	}
	if (result->ident_index == NULL)
		elog(ERROR, "Failed to open identity index");

	/* Only initialize fields needed by ExecInsertIndexTuples(). */
#if PG_VERSION_NUM < 140000
	estate->es_result_relations = estate->es_result_relation_info =
		result->rri;
	estate->es_num_result_relations = 1;
#endif
	result->estate = estate;

	return result;
}

void
free_index_insert_state(IndexInsertState *iistate)
{
	ExecCloseIndices(iistate->rri);
	FreeExecutorState(iistate->estate);
	pfree(iistate->rri);
	pfree(iistate);
}

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = plugin_startup;
	cb->begin_cb = plugin_begin_txn;
	cb->change_cb = plugin_change;
	cb->commit_cb = plugin_commit_txn;
	cb->filter_by_origin_cb = plugin_filter;
	cb->shutdown_cb = plugin_shutdown;
}


/* initialize this plugin */
static void
plugin_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			   bool is_init)
{
	ctx->output_plugin_private = NULL;

	/* Probably unnecessary, as we don't use the SQL interface ... */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	if (ctx->output_plugin_options != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("This plugin does not expect any options")));
	}
}

static void
plugin_shutdown(LogicalDecodingContext *ctx)
{
}

/*
 * As we don't release the slot during processing of particular table, there's
 * no room for SQL interface, even for debugging purposes. Therefore we need
 * neither OutputPluginPrepareWrite() nor OutputPluginWrite() in the plugin
 * callbacks. (Although we might want to write custom callbacks, this API
 * seems to be unnecessarily generic for our purposes.)
 */

/* BEGIN callback */
static void
plugin_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
}

/* COMMIT callback */
static void
plugin_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  XLogRecPtr commit_lsn)
{
}

/*
 * Callback for individual changed tuples
 */
static void
plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			  Relation relation, ReorderBufferChange *change)
{
	DecodingOutputState *dstate;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/* Only interested in one particular relation. */
	if (relation->rd_id != dstate->relid)
		return;

	/* Decode entry depending on its type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	newtuple;

				newtuple = change->data.tp.newtuple != NULL ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.newtuple : NULL;
#else
					&change->data.tp.newtuple->tuple : NULL;
#endif

				/*
				 * Identity checks in the main function should have made this
				 * impossible.
				 */
				if (newtuple == NULL)
					elog(ERROR, "Incomplete insert info.");

				store_change(ctx, PG_SQUEEZE_CHANGE_INSERT, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple,
							newtuple;

				oldtuple = change->data.tp.oldtuple != NULL ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.oldtuple : NULL;
#else
					&change->data.tp.oldtuple->tuple : NULL;
#endif
				newtuple = change->data.tp.newtuple != NULL ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.newtuple : NULL;
#else
					&change->data.tp.newtuple->tuple : NULL;
#endif

				if (newtuple == NULL)
					elog(ERROR, "Incomplete update info.");

				if (oldtuple != NULL)
					store_change(ctx, PG_SQUEEZE_CHANGE_UPDATE_OLD, oldtuple);

				store_change(ctx, PG_SQUEEZE_CHANGE_UPDATE_NEW, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple	oldtuple;

				oldtuple = change->data.tp.oldtuple ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.oldtuple : NULL;
#else
					&change->data.tp.oldtuple->tuple : NULL;
#endif

				if (oldtuple == NULL)
					elog(ERROR, "Incomplete delete info.");

				store_change(ctx, PG_SQUEEZE_CHANGE_DELETE, oldtuple);
			}
			break;
		default:
			/* Should not come here */
			Assert(0);
			break;
	}
}

/* Store concurrent data change. */
static void
store_change(LogicalDecodingContext *ctx, ConcurrentChangeKind kind,
			 HeapTuple tuple)
{
	DecodingOutputState *dstate;
	char	   *change_raw;
	ConcurrentChange *change;
	MemoryContext oldcontext;
	bool		flattened = false;
	Size		size;
	Datum		values[1];
	bool		isnull[1];
	char	   *dst;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/*
	 * ReorderBufferCommit() stores the TOAST chunks in its private memory
	 * context and frees them after having called apply_change(). Therefore we
	 * need flat copy (including TOAST) that we eventually copy into the
	 * memory context which is available to decode_concurrent_changes().
	 */
	if (HeapTupleHasExternal(tuple))
	{
		/*
		 * toast_flatten_tuple_to_datum() might be more convenient but we
		 * don't want the decompression it does.
		 */
		tuple = toast_flatten_tuple(tuple, dstate->tupdesc);
		flattened = true;
	}

	size = MAXALIGN(VARHDRSZ) + sizeof(ConcurrentChange) + tuple->t_len;
	/* XXX Isn't there any function / macro to do this? */
	if (size >= 0x3FFFFFFF)
		elog(ERROR, "Change is too big.");

	oldcontext = MemoryContextSwitchTo(ctx->context);
	change_raw = (char *) palloc(size);
	MemoryContextSwitchTo(oldcontext);

	SET_VARSIZE(change_raw, size);
	change = (ConcurrentChange *) VARDATA(change_raw);

	/*
	 * Copy the tuple.
	 *
	 * CAUTION: change->tup_data.t_data must be fixed on retrieval!
	 */
	memcpy(&change->tup_data, tuple, sizeof(HeapTupleData));
	dst = (char *) change + sizeof(ConcurrentChange);
	memcpy(dst, tuple->t_data, tuple->t_len);

	/* The other field. */
	change->kind = kind;

	/* The data has been copied. */
	if (flattened)
		pfree(tuple);

	/* Store as tuple of 1 bytea column. */
	values[0] = PointerGetDatum(change_raw);
	isnull[0] = false;
	tuplestore_putvalues(dstate->tstore, dstate->tupdesc_change,
						 values, isnull);

	/* Accounting. */
	dstate->nchanges++;

	/* Cleanup. */
	pfree(change_raw);
}

/*
 * Retrieve tuple from a change structure. As for the change, no alignment is
 * assumed.
 */
static HeapTuple
get_changed_tuple(ConcurrentChange *change)
{
	HeapTupleData tup_data;
	HeapTuple	result;
	char	   *src;

	/*
	 * Ensure alignment before accessing the fields. (This is why we can't use
	 * heap_copytuple() instead of this function.)
	 */
	memcpy(&tup_data, &change->tup_data, sizeof(HeapTupleData));

	result = (HeapTuple) palloc(HEAPTUPLESIZE + tup_data.t_len);
	memcpy(result, &tup_data, sizeof(HeapTupleData));
	result->t_data = (HeapTupleHeader) ((char *) result + HEAPTUPLESIZE);
	src = (char *) change + sizeof(ConcurrentChange);
	memcpy(result->t_data, src, result->t_len);

	return result;
}

/*
 * A filter that recognizes changes produced by the initial load.
 */
static bool
plugin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	DecodingOutputState *dstate;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/* dstate is not initialized during decoding setup - should it be? */
	if (dstate && dstate->rorigin != InvalidRepOriginId &&
		origin_id == dstate->rorigin)
		return true;

	return false;
}
