#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

typedef enum
{
	PG_SQUEEZE_CHANGE_INSERT,
	PG_SQUEEZE_CHANGE_UPDATE_OLD,
	PG_SQUEEZE_CHANGE_UPDATE_NEW,
	PG_SQUEEZE_CHANGE_DELETE
} ConcurrentChangeKind;

typedef struct ConcurrentChange
{
	/* The actual data. */
	HeapTuple	tuple;

	/* See the enum above. */
	ConcurrentChangeKind	kind;
} ConcurrentChange;

typedef struct DecodingOutputState
{
	/* The relation whose changes we're decoding. */
	Oid	relid;

	/* Tuple descriptor used by output plugin to form tuples. */
	TupleDesc	tupdesc;

	/*
	 * Change storage.
	 *
	 * TODO Store the flattened tuples in a tuplestore as Datums (the actual
	 * tuple can't be inserted since tuplestore only seems to support "minimal
	 * tuple", and so we'd be unable to transfer OID system column). The
	 * problem of purely in-memory storage is the output plugin receives all
	 * changes when commit record is decoded, so huge transactions cannot be
	 * processed in multiple steps. Even then we should try not to process all
	 * transactions at once, so that the tuplestore does not have to use disk
	 * too often. While doing so, should use the "data_size" field of this
	 * structure or some internal field of the tuplestore?
	 */
	ConcurrentChange	*changes;

	/* Number of changes currently stored. */
	int	nchanges;

	/* The size of "changes" array. */
	int	nchanges_max;

	/* Total amount of space used by "change tuples". */
	Size	data_size;

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

/*
 * Information on source relation index, used to build the index on the
 * transient relation. To avoid repeated retrieval of the pg_index fields we
 * also add pg_class(xmin) and pass the same structure to
 * check_catalog_changes().
 */
typedef struct IndexCatInfo
{
	Oid	oid;					/* pg_index(indexrelid) */
	NameData	relname;		/* pg_class(relname) */
	Oid	reltablespace;			/* pg_class(reltablespace) */
	TransactionId		xmin;	/* pg_index(xmin) */
	TransactionId		pg_class_xmin; /* pg_class(xmin) of the index (not the
										* parent relation) */
} IndexCatInfo;

/*
 * Information to check whether an "incompatible" catalog change took
 * place. Such a change prevents us from completing processing of the current
 * table.
 */
typedef struct CatalogState
{
	/* The relation whose changes we'll check for. */
	Oid	relid;

	/*
	 * Have both the source relation and its TOAST relation the
	 * "user_catalog_table" option set?
	 */
	bool		is_catalog;

	/* Copy of pg_class tuple. */
	Form_pg_class	form_class;

	/* Copy of pg_class tuple descriptor. */
	TupleDesc	desc_class;

	/* Copy of pg_class(reloptions). */
	Datum	reloptions;

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

	/* The same for TOAST relation, if there's one. */
	TransactionId		toast_xmin;

	/*
	 * Does at least one index wrong value of indisvalid, indisready or
	 * indislive?
	 */
	bool	invalid_index;
} CatalogState;

extern void check_catalog_changes(CatalogState *state, LOCKMODE lock_held);

extern IndexInsertState *get_index_insert_state(Relation relation,
												Oid ident_index_id);
extern void free_index_insert_state(IndexInsertState *iistate);
extern void process_concurrent_changes(LogicalDecodingContext *ctx,
									   XLogRecPtr *startptr,
									   XLogRecPtr end_of_wal,
									   CatalogState	*cat_state,
									   Relation rel_dst, ScanKey ident_key,
									   int ident_key_nentries,
									   IndexInsertState *iistate);
extern void	_PG_output_plugin_init(OutputPluginCallbacks *cb);
