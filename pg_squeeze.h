#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

typedef enum
{
	PG_SQUEEZE_CHANGE_INSERT,
	PG_SQUEEZE_CHANGE_UPDATE_OLD,
	PG_SQUEEZE_CHANGE_UPDATE_NEW,
	PG_SQUEEZE_CHANGE_DELETE
} ChangeKind;

typedef struct ChangeStore
{
	TupleDesc	tupdesc;
	Tuplestorestate	*tupstore;
} ChangeStore;

typedef struct DecodingOutputState
{
	/* The relation whose changes we're decoding. */
	Oid	relid;

	/* The actual data decoded. */
	ChangeStore	data;

	/*
	 * One byte per regular data item, telling what kind of change it
	 * represents. An in-memory array would be a little bit easier to
	 * implement, but in that case we'd have to pay special attention to
	 * extreme cases (in terms of number of changes), while tuplestore stores
	 * data to disk in transparent way.
	 *
	 * XXX Consider storing multiple values per tuple, to conserve space.
	 */
	ChangeStore	metadata;

	ResourceOwner	resowner;
} DecodingOutputState;

extern void	_PG_init(void);

extern int squeeze_worker_naptime;

/* Everything we need to call ExecInsertIndexTuples(). */
typedef struct IndexInsertState
{
	ResultRelInfo	*rri;
	EState	*estate;
	ExprContext	*econtext;

	/*
	 * This field is not necessary for index updates, but it's convenient
	 * to open / close it along with the other indexes.
	 */
	Relation	ident_index;
} IndexInsertState;

extern void decode_concurrent_changes(LogicalDecodingContext *ctx,
									  XLogRecPtr *startptr,
									  XLogRecPtr end_of_wal);
extern IndexInsertState *get_index_insert_state(Relation relation,
												Oid ident_index_id);
extern void free_index_insert_state(IndexInsertState *iistate);
extern void process_concurrent_changes(DecodingOutputState *dstate,
									   Relation relation, ScanKey key,
									   int nkeys, IndexInsertState *iistate);
extern void	_PG_output_plugin_init(OutputPluginCallbacks *cb);
