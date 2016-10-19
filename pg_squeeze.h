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
#include "postmaster/bgworker.h"
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
	/* See the enum above. */
	ConcurrentChangeKind	kind;

	/*
	 * The actual tuple.
	 *
	 * The tuple data follows the ConcurrentChange structure. Before use make
	 * sure the tuple is correctly aligned (ConcurrentChange can be stored as
	 * bytea) and that tuple->t_data is fixed.
	 */
	HeapTupleData	tup_data;
} ConcurrentChange;

typedef struct DecodingOutputState
{
	/* The relation whose changes we're decoding. */
	Oid	relid;

	/*
	 * Decoded changes are stored here. Although we try to avoid excessive
	 * batches, it can happen that the changes need to be stored to disk. The
	 * tuplestore does this transparently.
	 */
	Tuplestorestate *tstore;

	/* The current number of changes in tstore. */
	double	nchanges;

	/*
	 * Descriptor to store the ConcurrentChange structure serialized
	 * (bytea). We can't store the tuple directly because tuplestore only
	 * supports minimum tuple and we may need to transfer OID system column
	 * from the output plugin. Also we need to transfer the change kind, so
	 * it's better to put everything in the structure than to use 2
	 * tuplestores "in parallel".
	 */
	TupleDesc	tupdesc_change;

	/* Tuple descriptor needed to update indexes. */
	TupleDesc	tupdesc;

	/* Slot to retrieve data from tstore. */
	TupleTableSlot	*tsslot;

	/*
	 * Total amount of space used by "change tuples". We use this field to
	 * minimize the likelihood that tstore will have to be spilled to
	 * disk. (Such a spilling should only be necessary for huge transactions,
	 * because decoding of these cannot be split into multiple steps.)
	 */
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
extern bool process_concurrent_changes(LogicalDecodingContext *ctx,
									   XLogRecPtr *startptr,
									   XLogRecPtr end_of_wal,
									   CatalogState	*cat_state,
									   Relation rel_dst, ScanKey ident_key,
									   int ident_key_nentries,
									   IndexInsertState *iistate,
									   LOCKMODE lock_held,
									   struct timeval *must_complete);
extern void	_PG_output_plugin_init(OutputPluginCallbacks *cb);

extern void squeeze_initialize_bgworker(BackgroundWorker *worker,
										bgworker_main_type bgw_main, Oid db,
										Oid user, Oid notify_pid);
extern void squeeze_worker_main(Datum main_arg);
