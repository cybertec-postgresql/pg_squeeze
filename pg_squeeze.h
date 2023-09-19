/*-----------------------------------------------------
 *
 * pg_squeeze.h
 *     A tool to eliminate table bloat.
 *
 * Copyright (c) 2016-2023, CYBERTEC PostgreSQL International GmbH
 *
 *-----------------------------------------------------
 */
#include <sys/time.h>

#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#if PG_VERSION_NUM < 130000
#include "access/tuptoaster.h"
#endif
#include "access/xlog_internal.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "nodes/execnodes.h"
#include "postmaster/bgworker.h"
#include "replication/logical.h"
#include "replication/origin.h"
#include "storage/ipc.h"
#include "utils/array.h"
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
	ConcurrentChangeKind kind;

	/*
	 * The actual tuple.
	 *
	 * The tuple data follows the ConcurrentChange structure. Before use make
	 * sure the tuple is correctly aligned (ConcurrentChange can be stored as
	 * bytea) and that tuple->t_data is fixed.
	 */
	HeapTupleData tup_data;
} ConcurrentChange;

typedef struct DecodingOutputState
{
	/* The relation whose changes we're decoding. */
	Oid			relid;

	/*
	 * Decoded changes are stored here. Although we try to avoid excessive
	 * batches, it can happen that the changes need to be stored to disk. The
	 * tuplestore does this transparently.
	 */
	Tuplestorestate *tstore;

	/* The current number of changes in tstore. */
	double		nchanges;

	/*
	 * Descriptor to store the ConcurrentChange structure serialized (bytea).
	 * We can't store the tuple directly because tuplestore only supports
	 * minimum tuple and we may need to transfer OID system column from the
	 * output plugin. Also we need to transfer the change kind, so it's better
	 * to put everything in the structure than to use 2 tuplestores "in
	 * parallel".
	 */
	TupleDesc	tupdesc_change;

	/* Tuple descriptor needed to update indexes. */
	TupleDesc	tupdesc;

	/* Slot to retrieve data from tstore. */
	TupleTableSlot *tsslot;

	/*
	 * WAL records having this origin have been created by the initial load
	 * and should not be decoded.
	 */
	RepOriginId rorigin;

	ResourceOwner resowner;
} DecodingOutputState;

/* The WAL segment being decoded. */
extern XLogSegNo squeeze_current_segment;

extern void _PG_init(void);

/* Everything we need to call ExecInsertIndexTuples(). */
typedef struct IndexInsertState
{
	ResultRelInfo *rri;
	EState	   *estate;
	ExprContext *econtext;

	Relation	ident_index;
} IndexInsertState;

/*
 * Subset of fields of pg_class, plus the necessary info on attributes. It
 * represents either the source relation or a composite type of the source
 * relation's attribute.
 */
typedef struct PgClassCatInfo
{
	/* pg_class(oid) */
	Oid			relid;

	/*
	 * pg_class(xmin)
	 */
	TransactionId xmin;

	/* Array of pg_attribute(xmin). (Dropped columns are here too.) */
	TransactionId *attr_xmins;
	int16		relnatts;
} PgClassCatInfo;

/*
 * Information on source relation index, used to build the index on the
 * transient relation. To avoid repeated retrieval of the pg_index fields we
 * also add pg_class(xmin) and pass the same structure to
 * check_catalog_changes().
 */
typedef struct IndexCatInfo
{
	Oid			oid;			/* pg_index(indexrelid) */
	NameData	relname;		/* pg_class(relname) */
	Oid			reltablespace;	/* pg_class(reltablespace) */
	TransactionId xmin;			/* pg_index(xmin) */
	TransactionId pg_class_xmin;	/* pg_class(xmin) of the index (not the
									 * parent relation) */
} IndexCatInfo;

/*
 * If the source relation has attribute(s) of composite type, we need to check
 * for changes of those types.
 */
typedef struct TypeCatInfo
{
	Oid			oid;			/* pg_type(oid) */
	TransactionId xmin;			/* pg_type(xmin) */

	/*
	 * The pg_class entry whose oid == pg_type(typrelid) of this type.
	 */
	PgClassCatInfo rel;
} TypeCatInfo;

/*
 * Information to check whether an "incompatible" catalog change took
 * place. Such a change prevents us from completing processing of the current
 * table.
 */
typedef struct CatalogState
{
	/* The relation whose changes we'll check for. */
	PgClassCatInfo rel;

	/* Copy of pg_class tuple of the source relation. */
	Form_pg_class form_class;

	/* Copy of pg_class tuple descriptor of the source relation. */
	TupleDesc	desc_class;

	/* Per-index info. */
	int			relninds;
	IndexCatInfo *indexes;

	/* Composite types used by the source rel attributes. */
	TypeCatInfo *comptypes;
	/* Size of the array. */
	int			ncomptypes_max;
	/* Used elements of the array. */
	int			ncomptypes;

	/*
	 * Does at least one index have wrong value of indisvalid, indisready or
	 * indislive?
	 */
	bool		invalid_index;

	/* Does the table have primary key index? */
	bool		have_pk_index;
} CatalogState;

extern void check_catalog_changes(CatalogState *state, LOCKMODE lock_held);

extern IndexInsertState *get_index_insert_state(Relation relation,
												Oid ident_index_id);
extern void free_index_insert_state(IndexInsertState *iistate);
extern bool process_concurrent_changes(LogicalDecodingContext *ctx,
									   XLogRecPtr end_of_wal,
									   CatalogState *cat_state,
									   Relation rel_dst, ScanKey ident_key,
									   int ident_key_nentries,
									   IndexInsertState *iistate,
									   LOCKMODE lock_held,
									   struct timeval *must_complete);
extern bool decode_concurrent_changes(LogicalDecodingContext *ctx,
									  XLogRecPtr end_of_wal,
									  struct timeval *must_complete);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

#if PG_VERSION_NUM >= 150000
extern shmem_request_hook_type prev_shmem_request_hook;
#endif
extern shmem_startup_hook_type prev_shmem_startup_hook;

extern int	squeeze_workers_per_database;

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
	char	   *dbname;
	char	   *rolename;
	bool		scheduler;
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
	Oid			dbid;
	Oid			roleid;
	bool		scheduler;

	int			task_id;
} WorkerConInteractive;

/* Progress tracking. */
typedef struct WorkerProgress
{
	slock_t		mutex;

	/* Tuples inserted during the initial load. */
	int64		ins_initial;

	/*
	 * Tuples inserted, updated and deleted after the initial load (i.e.
	 * during the catch-up phase).
	 */
	int64		ins;
	int64		upd;
	int64		del;
} WorkerProgress;

/*
 * Shared memory structures to keep track of the status of squeeze workers.
 */
typedef struct WorkerSlot
{
	Oid			dbid;			/* database the worker is connected to */
	Oid			relid;			/* relation the worker is working on */
	int			pid;			/* the PID */
	bool		scheduler;		/* true if scheduler, false if the "squeeze
								 * worker" */
	WorkerProgress progress;	/* progress tracking information */
	Latch	   *latch;			/* use this to wake up the worker */
} WorkerSlot;

/*
 * This structure represents a task assigned to the worker via shared memory.
 */
typedef struct WorkerTask
{
	/* Backend that assigned the task both sets and clears this field. */
	bool		assigned;
	/* See the comments of exit_if_requested(). */
	bool	exit_requested;
	/* Worker that performs the task both sets and clears this field. */
	WorkerSlot	*slot;

	/*
	 * Use this when setting / clearing the fields above. Once "assigned" is
	 * set, the task belongs to the backend that set it, so the other fields
	 * may be assigned w/o the lock.
	 */
	slock_t		mutex;

	NameData	relschema;
	NameData	relname;
	NameData	indname;		/* clustering index */
	NameData	tbspname;		/* destination tablespace */

	/*
	 * index destination tablespaces.
	 *
	 * text[][] array is stored here. The space should only be used by the
	 * interactive squeeze_table() function, which is only there for testing
	 * and troubleshooting purposes. If the array doesn't fit here, the user
	 * needs to use the regular UI (ie register the table for squeezing and
	 * insert a record into the "tasks" table).
	 */
#define IND_TABLESPACES_ARRAY_SIZE	1024
	char		ind_tbsps[IND_TABLESPACES_ARRAY_SIZE];
} WorkerTask;

extern WorkerTask *MyWorkerTask;
extern WorkerProgress *MyWorkerProgress;

extern WorkerConInit *allocate_worker_con_info(char *dbname,
											   char *rolename,
											   bool scheduler);
extern void squeeze_initialize_bgworker(BackgroundWorker *worker,
										WorkerConInit *con_init,
										WorkerConInteractive *con_interactive,
										pid_t notify_pid);

extern void squeeze_worker_shmem_request(void);
extern void squeeze_worker_shmem_startup(void);

extern PGDLLEXPORT void squeeze_worker_main(Datum main_arg);

extern void exit_if_requested(void);
extern bool squeeze_table_impl(Name relschema, Name relname, Name indname,
							   Name tbspname, ArrayType *ind_tbsp,
							   ErrorData **edata_p, MemoryContext edata_cxt);
extern void squeeze_handle_error_db(ErrorData **edata_p,
									MemoryContext edata_cxt);
