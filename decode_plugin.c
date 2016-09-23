/*-------------------------------------------------------------------------
 *
 * decode_plugin.c
 *
 *		Logical decoding output plugin to retrieve concurrent changes while
 *		tuples are being copied into a new table.
 *-------------------------------------------------------------------------
 */
/*
 * TODO Remove unused includes.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_squeeze.h"

/* This must be available to pg_dlsym(). */
extern void	_PG_output_plugin_init(OutputPluginCallbacks *cb);

static void plugin_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
							  bool is_init);
static void plugin_shutdown(LogicalDecodingContext *ctx);
static void plugin_begin_txn(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn);
static void plugin_commit_txn(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation rel, ReorderBufferChange *change);

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = plugin_startup;
	cb->begin_cb = plugin_begin_txn;
	cb->change_cb = plugin_change;
	cb->commit_cb = plugin_commit_txn;
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
	DecodingOutputState	*dstate;
	Datum	values[1];
	bool	isnull[1];

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/* Only interested in one particular relation. */
	if (relation->rd_id != dstate->relid)
		return;

	isnull[0] = false;

	/* Decode entry depending on its type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	newtuple;

				newtuple = change->data.tp.newtuple != NULL ?
					&change->data.tp.newtuple->tuple : NULL;

				/*
				 * Identity checks in the main function should have made this
				 * impossible.
				 */
				if (newtuple == NULL)
					elog(ERROR, "Incomplete insert info.");

				/* Store the tuple itself ... */
				tuplestore_puttuple(dstate->data.tupstore, newtuple);

				/* ... and the info on the change kind. */
				values[0] = CharGetDatum(PG_SQUEEZE_CHANGE_INSERT);
				tuplestore_putvalues(dstate->metadata.tupstore,
									 dstate->metadata.tupdesc,
									 values, isnull);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple oldtuple, newtuple;

				oldtuple = change->data.tp.oldtuple != NULL ?
					&change->data.tp.oldtuple->tuple : NULL;
				newtuple = change->data.tp.newtuple != NULL ?
					&change->data.tp.newtuple->tuple : NULL;

				if (newtuple == NULL)
					elog(ERROR, "Incomplete update info.");

				if (oldtuple != NULL)
				{
					tuplestore_puttuple(dstate->data.tupstore, oldtuple);

					values[0] = CharGetDatum(PG_SQUEEZE_CHANGE_UPDATE_OLD);
					tuplestore_putvalues(dstate->metadata.tupstore,
										 dstate->metadata.tupdesc,
										 values, isnull);
				}

				tuplestore_puttuple(dstate->data.tupstore, newtuple);

				values[0] = CharGetDatum(PG_SQUEEZE_CHANGE_UPDATE_NEW);
				tuplestore_putvalues(dstate->metadata.tupstore,
									 dstate->metadata.tupdesc,
									 values, isnull);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple oldtuple;

				oldtuple = change->data.tp.oldtuple ?
					&change->data.tp.oldtuple->tuple : NULL;

				if (oldtuple == NULL)
					elog(ERROR, "Incomplete delete info.");

				tuplestore_puttuple(dstate->data.tupstore, oldtuple);

				values[0] = CharGetDatum(PG_SQUEEZE_CHANGE_DELETE);
				tuplestore_putvalues(dstate->metadata.tupstore,
									 dstate->metadata.tupdesc,
									 values, isnull);
			}
			break;
		default:
			/* Should not come here */
			Assert(0);
			break;
	}
}
