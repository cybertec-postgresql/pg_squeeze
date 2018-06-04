/*-----------------------------------------------------
 *
 * pg_squeeze.h
 *     A tool to eliminate table bloat.
 *
 * Copyright (c) 2016-2018, Cybertec Schönig & Schönig GmbH
 *
 *-----------------------------------------------------
 */
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
#include "replication/snapbuild.h"
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
									   XLogRecPtr end_of_wal,
									   CatalogState	*cat_state,
									   Relation rel_dst, ScanKey ident_key,
									   int ident_key_nentries,
									   IndexInsertState *iistate,
									   LOCKMODE lock_held,
									   struct timeval *must_complete);
extern void	_PG_output_plugin_init(OutputPluginCallbacks *cb);

/*
 * Connection information the squeeze worker needs to connect to database if
 * starting automatically. Strings are more convenient for admin than OIDs and
 * we have no chance to lookup OIDs in the catalog when registering worker
 * during postmaster startup. That's why we pass strings.
 *
 * The structure is allocated in TopMemoryContext during postmaster startup,
 * so the worker should access it correctly if it receives pointer from the
 * bgw_main_arg field of BackgroundWorker.
 */
typedef struct WorkerConInit
{
	char	*dbname;
	char	*rolename;
} WorkerConInit;

/*
 * The same for interactive start of the worker. In this case we can no longer
 * add anything to the TopMemoryContext of postmaster, so
 * BackgroundWorker.bgw_extra is the only way to pass the information. As we
 * have OIDs at this stage, the structure is small enough to fit bgw_extra
 * field of BackgroundWorker.
 */
typedef struct WorkerConInteractive
{
	Oid	dbid;
	Oid	roleid;
} WorkerConInteractive;

extern WorkerConInit *allocate_worker_con_info(char *dbname,
											   char *rolename);
extern void squeeze_initialize_bgworker(BackgroundWorker *worker,
										bgworker_main_type bgw_main,
										WorkerConInit *con_init,
										WorkerConInteractive *con_interactive,
										Oid notify_pid);
extern void squeeze_worker_main(Datum main_arg);

/*
 * This struct contains the current state of the snapshot building
 * machinery. Besides a forward declaration in the header, it is not exposed
 * to the public, so we can easily change its contents.
 */
struct SnapBuild
{
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions < than this have committed/aborted */
	TransactionId xmin;

	/* all transactions >= than this are uncommitted */
	TransactionId xmax;

	/*
	 * Don't replay commits from an LSN < this LSN. This can be set externally
	 * but it will also be advanced (never retreat) from within snapbuild.c.
	 */
	XLogRecPtr	start_decoding_at;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with an xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;

	/* Indicates if we are building full snapshot or just catalog one. */
	bool		building_full_snapshot;

	/*
	 * Snapshot that's valid to see the catalog state seen at this moment.
	 */
	Snapshot	snapshot;

	/*
	 * LSN of the last location we are sure a snapshot has been serialized to.
	 */
	XLogRecPtr	last_serialized_snapshot;

	/*
	 * The reorderbuffer we need to update with usable snapshots et al.
	 */
	ReorderBuffer *reorder;

	/*
	 * Outdated: This struct isn't used for its original purpose anymore, but
	 * can't be removed / changed in a minor version, because it's stored
	 * on-disk.
	 */
	struct
	{
		/*
		 * NB: This field is misused, until a major version can break on-disk
		 * compatibility. See SnapBuildNextPhaseAt() /
		 * SnapBuildStartNextPhaseAt().
		 */
		TransactionId was_xmin;
		TransactionId was_xmax;

		size_t		was_xcnt;	/* number of used xip entries */
		size_t		was_xcnt_space; /* allocated size of xip */
		TransactionId *was_xip; /* running xacts array, xidComparator-sorted */
	}			was_running;

	/*
	 * Array of transactions which could have catalog changes that committed
	 * between xmin and xmax.
	 */
	struct
	{
		/* number of committed transactions */
		size_t		xcnt;

		/* available space for committed transactions */
		size_t		xcnt_space;

		/*
		 * Until we reach a CONSISTENT state, we record commits of all
		 * transactions, not just the catalog changing ones. Record when that
		 * changes so we know we cannot export a snapshot safely anymore.
		 */
		bool		includes_all_transactions;

		/*
		 * Array of committed transactions that have modified the catalog.
		 *
		 * As this array is frequently modified we do *not* keep it in
		 * xidComparator order. Instead we sort the array when building &
		 * distributing a snapshot.
		 *
		 * TODO: It's unclear whether that reasoning has much merit. Every
		 * time we add something here after becoming consistent will also
		 * require distributing a snapshot. Storing them sorted would
		 * potentially also make it easier to purge (but more complicated wrt
		 * wraparound?). Should be improved if sorting while building the
		 * snapshot shows up in profiles.
		 */
		TransactionId *xip;
	}			committed;
};

extern Snapshot SnapBuildInitialSnapshot(SnapBuild *builder);
