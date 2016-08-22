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
} DecodingOutputState;
