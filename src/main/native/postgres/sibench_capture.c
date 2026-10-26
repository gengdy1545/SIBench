/*
 * PostgreSQL 14.24 capture instrumentation, linked into the backend (see sibench_capture.h).
 *
 * Every event is one JSON line in SIBENCH_CAPTURE_DIR/pg-<pid>.jsonl. The first line of a file is
 * capture_start, naming the format and whether per-row detail is kept.
 *
 * An index access unit starts with a scan_rescan and ends before the next rescan, the scan's end,
 * a transaction abort or backend exit. Its facts are accumulated here and written as one
 * scan_summary right before that event, so the ledger applies them to the unit it is about to
 * settle. A bitmap heap node's verdicts are aggregated by the set of input scans whose current
 * unit produced the TID (bitmap_verdicts), written before any of its inputs changes unit; that is
 * exactly the attribution the ledger makes per TID.
 */
#include "postgres.h"
#include "sibench_capture.h"

#include <math.h>
#include <stdio.h>
#include <unistd.h>
#include <zstd.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "executor/execdesc.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#define SIB_FORMAT 2
#define SIB_MAX_INPUTS 64
#define SIB_PENDING_SETS 16

IndexScanDesc sibench_bitmap_scan = NULL;
void	   *sibench_bitmap_heap_node = NULL;
int			sibench_depth = 0;
uint64		sibench_request = 0;

static FILE *sib_stream = NULL;
static int	sib_detail_mode = -1;

/*
 * With SIBENCH_CAPTURE_COMPRESS=zstd the trace is one zstd stream, pg-<pid>.jsonl.zst. It is
 * flushed at every transaction end, like the plain file, and its frame is ended at backend exit; a
 * reader of a file whose backend is still running finds no frame end and fails.
 */
static ZSTD_CCtx *sib_zstd = NULL;
static char *sib_zstd_out = NULL;
static size_t sib_zstd_out_size = 0;
static bool sib_zstd_open_frame = false;

/* ------------------------------------------------------------------------------------------ */
/* Output stream                                                                              */
/* ------------------------------------------------------------------------------------------ */

bool
sib_enabled(void)
{
	return application_name && strncmp(application_name, "sibench:", 8) == 0;
}

bool
sib_relation(Relation relation)
{
	return sib_enabled() && relation && RelationGetRelid(relation) >= FirstNormalObjectId;
}

/* Per-row events are kept only on request: they are what makes a capture large and slow. */
static bool
sib_detail(void)
{
	if (sib_detail_mode < 0)
	{
		const char *value = getenv("SIBENCH_CAPTURE_DETAIL");

		sib_detail_mode = value && strcmp(value, "rows") == 0;
	}
	return sib_detail_mode == 1;
}

static void sib_flush_all(void);

/* Feeds input to the zstd stream with the given directive and writes what it produced. */
static bool
sib_zstd_feed(const char *data, size_t length, ZSTD_EndDirective directive)
{
	ZSTD_inBuffer input = {data, length, 0};
	size_t		remaining;

	do
	{
		ZSTD_outBuffer output = {sib_zstd_out, sib_zstd_out_size, 0};

		remaining = ZSTD_compressStream2(sib_zstd, &output, &input, directive);
		if (ZSTD_isError(remaining))
			return false;
		if (output.pos > 0 && fwrite(sib_zstd_out, 1, output.pos, sib_stream) != output.pos)
			return false;
	} while (directive == ZSTD_e_continue ? input.pos < input.size : remaining != 0);
	return true;
}

static void
sib_write(const char *data, size_t length)
{
	if (sib_zstd)
	{
		sib_zstd_open_frame = true;
		if (!sib_zstd_feed(data, length, ZSTD_e_continue))
			elog(ERROR, "SIBench trace write failed");
		return;
	}
	if (fwrite(data, 1, length, sib_stream) != length)
		elog(ERROR, "SIBench trace write failed");
}

/* Ends the zstd frame; anything written later starts a new frame of the same stream. */
static void
sib_finish(void)
{
	if (!sib_stream)
		return;
	if (sib_zstd && sib_zstd_open_frame)
	{
		sib_zstd_feed(NULL, 0, ZSTD_e_end);
		sib_zstd_open_frame = false;
	}
	fflush(sib_stream);
}

/* Open units and pending verdicts are still facts when the connection closes mid-request. */
static void
sib_before_exit(int code, Datum arg)
{
	sib_flush_all();
	sib_finish();
}

static void
sib_proc_exit(int code, Datum arg)
{
	sib_finish();
}

static bool
sib_open(void)
{
	const char *directory = getenv("SIBENCH_CAPTURE_DIR");
	char		path[MAXPGPATH];
	StringInfoData header;

	if (sib_stream)
		return true;
	if (!directory || !directory[0])
		return false;
	{
		const char *compress = getenv("SIBENCH_CAPTURE_COMPRESS");
		bool		zstd = compress && strcmp(compress, "zstd") == 0;

		snprintf(path, sizeof(path), "%s/pg-%d.jsonl%s", directory, MyProcPid, zstd ? ".zst" : "");
		sib_stream = fopen(path, "a");
		if (!sib_stream)
			elog(ERROR, "SIBench cannot open trace file %s", path);
		if (zstd)
		{
			sib_zstd = ZSTD_createCCtx();
			/* The fastest regular level: compression runs inside every captured backend. */
			if (!sib_zstd || ZSTD_isError(ZSTD_CCtx_setParameter(sib_zstd, ZSTD_c_compressionLevel, 1)))
				elog(ERROR, "SIBench cannot start zstd for %s", path);
			sib_zstd_out_size = ZSTD_CStreamOutSize();
			sib_zstd_out = MemoryContextAlloc(TopMemoryContext, sib_zstd_out_size);
		}
	}
	/*
	 * Written out only when buffers fill and at exit, where the zstd frame ends: the audit waits
	 * for every backend to exit before it reads a trace, and a lost backend fails the capture
	 * anyway. A flush per transaction cost a zstd block and a write per commit on the CPUs the
	 * native run is bound by.
	 */
	setvbuf(sib_stream, NULL, _IOFBF, 1 << 20);
	before_shmem_exit(sib_before_exit, (Datum) 0);
	on_proc_exit(sib_proc_exit, (Datum) 0);
	initStringInfo(&header);
	appendStringInfo(&header,
					 "{\"event\":\"capture_start\",\"pid\":%d,\"xid\":0,\"cid\":0,\"req\":%llu,\"depth\":%d,"
					 "\"application\":",
					 MyProcPid, (unsigned long long) sibench_request, sibench_depth);
	escape_json(&header, application_name);
	appendStringInfo(&header, ",\"format\":%d,\"detail\":\"%s\"}\n", SIB_FORMAT,
					 sib_detail() ? "rows" : "summary");
	sib_write(header.data, header.len);
	pfree(header.data);
	return true;
}

/* A non-negative integer in decimal, as %u / %llu / %d print it, without printf. */
static inline void
sib_append_uint(StringInfo out, uint64 value)
{
	char		digits[MAXINT8LEN + 1];

	appendBinaryStringInfo(out, digits, pg_ulltoa_n(value, digits));
}

/* A 64-bit value as 16 lower-case hex digits, as %016llx prints it. */
static inline void
sib_append_hex16(StringInfo out, uint64 value)
{
	static const char hex[] = "0123456789abcdef";
	char		digits[16];
	int			i;

	for (i = 15; i >= 0; i--, value >>= 4)
		digits[i] = hex[value & 0xf];
	appendBinaryStringInfo(out, digits, 16);
}

/*
 * application_name as JSON. It changes once per request (the capture driver names each one) while
 * every event of the request repeats it, so its escaped form is kept until the name changes.
 */
static const char *
sib_application_json(void)
{
	static char *source = NULL;
	static StringInfoData escaped;
	const char *name = application_name ? application_name : "";

	if (source && strcmp(source, name) == 0)
		return escaped.data;
	if (!source)
	{
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

		initStringInfo(&escaped);
		MemoryContextSwitchTo(old);
	}
	else
		pfree(source);
	source = MemoryContextStrdup(TopMemoryContext, name);
	resetStringInfo(&escaped);
	escape_json(&escaped, name);
	return escaped.data;
}

static void
sib_emit_named(const char *event, Oid relid, const char *relname, const char *detail)
{
	static StringInfoData message;
	bool		executor_end,
				transaction_abort;

	if (!sib_enabled())
		return;
	/* Counters advance even when no capture directory is configured. */
	if (strcmp(event, "executor_start") == 0)
	{
		if (sibench_depth == 0)
			sibench_request++;
		sibench_depth++;
	}
	executor_end = strcmp(event, "executor_end") == 0;
	transaction_abort = strcmp(event, "transaction_abort") == 0;
	if (sib_open())
	{
		/* One buffer for every event of the backend; the fields are those printf wrote. */
		if (!message.data)
		{
			MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

			initStringInfo(&message);
			MemoryContextSwitchTo(old);
		}
		resetStringInfo(&message);
		appendStringInfoString(&message, "{\"event\":\"");
		appendStringInfoString(&message, event);
		appendStringInfoString(&message, "\",\"pid\":");
		sib_append_uint(&message, (uint64) MyProcPid);
		appendStringInfoString(&message, ",\"xid\":");
		sib_append_uint(&message, GetTopTransactionIdIfAny());
		appendStringInfoString(&message, ",\"cid\":");
		sib_append_uint(&message, GetCurrentCommandId(false));
		appendStringInfoString(&message, ",\"req\":");
		sib_append_uint(&message, sibench_request);
		appendStringInfoString(&message, ",\"depth\":");
		sib_append_uint(&message, (uint64) sibench_depth);
		appendStringInfoString(&message, ",\"application\":");
		appendStringInfoString(&message, sib_application_json());
		if (relname)
		{
			appendStringInfoString(&message, ",\"relation_oid\":");
			sib_append_uint(&message, relid);
			appendStringInfoString(&message, ",\"relation\":");
			escape_json(&message, relname);
		}
		if (detail && detail[0])
		{
			appendStringInfoChar(&message, ',');
			appendStringInfoString(&message, detail);
		}
		appendStringInfoString(&message, "}\n");
		sib_write(message.data, message.len);
	}
	/* An aborted transaction never reaches ExecutorEnd for its active executors. */
	if (executor_end && sibench_depth > 0)
		sibench_depth--;
	if (transaction_abort)
		sibench_depth = 0;
}

void
sib_emit(const char *event, Relation relation, const char *detail)
{
	if (relation)
		sib_emit_named(event, RelationGetRelid(relation), RelationGetRelationName(relation), detail);
	else
		sib_emit_named(event, InvalidOid, NULL, detail);
}

void
sib_datum(StringInfo out, Oid type, Datum datum, bool isnull)
{
	Oid			output;
	bool		variable;
	char	   *text;

	if (isnull)
	{
		appendStringInfoString(out, "null");
		return;
	}
	getTypeOutputInfo(type, &output, &variable);
	text = OidOutputFunctionCall(output, datum);
	escape_json(out, text);
	pfree(text);
}

/* Output through a resolved function: usable during abort and exit, when catalogs are not. */
static void
sib_datum_with(StringInfo out, FmgrInfo *output, Datum datum, bool isnull)
{
	char	   *text;

	if (isnull)
	{
		appendStringInfoString(out, "null");
		return;
	}
	text = OutputFunctionCall(output, datum);
	escape_json(out, text);
	pfree(text);
}

/* ------------------------------------------------------------------------------------------ */
/* Per-row detail events (SIBENCH_CAPTURE_DETAIL=rows) and scan lifecycle events               */
/* ------------------------------------------------------------------------------------------ */

/*
 * The elements of an array scan key (= ANY), so the scan's values are facts rather than an opaque
 * array. A row comparison has no single-column meaning and stays marked unsupported.
 */
static void
sib_scan_key(StringInfo out, ScanKey k, Oid type)
{
	ArrayType  *array;
	Oid			element;
	int16		length;
	bool		byval;
	char		align;
	Datum	   *values;
	bool	   *nulls;
	int			count,
				i;

	if (k->sk_flags & SK_ROW_HEADER)
	{
		appendStringInfoString(out, "null,\"unsupported_key\":true");
		return;
	}
	if (!(k->sk_flags & SK_SEARCHARRAY))
	{
		sib_datum(out, type, k->sk_argument, (k->sk_flags & SK_ISNULL) != 0);
		return;
	}
	appendStringInfoString(out, "null");
	if (k->sk_flags & SK_ISNULL)
	{
		appendStringInfoString(out, ",\"array\":null");
		return;
	}
	array = DatumGetArrayTypeP(k->sk_argument);
	element = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(element, &length, &byval, &align);
	deconstruct_array(array, element, length, byval, align, &values, &nulls, &count);
	appendStringInfo(out, ",\"array_type_oid\":%u,\"array\":[", element);
	for (i = 0; i < count; i++)
	{
		if (i)
			appendStringInfoChar(out, ',');
		sib_datum(out, element, values[i], nulls[i]);
	}
	appendStringInfoChar(out, ']');
}

/* The fields every index scan event shares: scan identity, snapshot, TID and scan keys. */
static void
sib_scan_detail(StringInfo out, IndexScanDesc scan, ScanKey keys, int nkeys,
				ItemPointer tid, int direction, bool found)
{
	int			i;

	appendStringInfo(out, "\"scan\":\"%p\",\"heap_oid\":%u,\"direction\":%d,\"found\":%s,\"recheck\":%s",
					 scan, scan->indexRelation->rd_index->indrelid, direction,
					 found ? "true" : "false", scan->xs_recheck ? "true" : "false");
	if (scan->xs_snapshot)
	{
		Snapshot	s = scan->xs_snapshot;

		/*
		 * Only MVCC snapshots carry xmin/xmax/xip; others (e.g. planner endpoint probes use a
		 * stack-allocated SnapshotNonVacuumable) leave those fields uninitialized. csn is the
		 * transaction completion count GetSnapshotData() read under ProcArrayLock together with
		 * xmin, xmax and xip: the counter is incremented under that lock exactly when a
		 * transaction stops counting as running, so a commit is visible to a snapshot iff the
		 * snapshot's counter is at or after the commit's. The snapshot carries it (sibenchCsn), so
		 * every copy a scan uses keeps it; a backend-wide "last snapshot built" would also follow
		 * catalog snapshots built after the statement's. An unknown counter is left out.
		 */
		if (s->snapshot_type == SNAPSHOT_MVCC)
		{
			appendStringInfo(out, ",\"snapshot_type\":%d", s->snapshot_type);
			if (s->sibenchCsn != 0)
				appendStringInfo(out, ",\"csn\":%llu", (unsigned long long) s->sibenchCsn);
			appendStringInfo(out, ",\"xmin\":%u,\"xmax\":%u,\"snapshot_cid\":%u,\"xip\":[",
							 s->xmin, s->xmax, s->curcid);
			for (i = 0; i < s->xcnt; i++)
				appendStringInfo(out, "%s%u", i ? "," : "", s->xip[i]);
			appendStringInfo(out, "],\"suboverflowed\":%s", s->suboverflowed ? "true" : "false");
		}
		else
			appendStringInfo(out, ",\"snapshot_type\":%d", s->snapshot_type);
	}
	if (tid && ItemPointerIsValid(tid))
		appendStringInfo(out, ",\"tid\":\"(%u,%u)\"", ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
	appendStringInfoString(out, ",\"keys\":[");
	for (i = 0; i < nkeys; i++)
	{
		ScanKey		k = keys + i;
		Oid			type = k->sk_subtype;

		if (!OidIsValid(type))
			type = TupleDescAttr(RelationGetDescr(scan->indexRelation), k->sk_attno - 1)->atttypid;
		appendStringInfo(out, "%s{\"attribute\":%d,\"strategy\":%d,\"flags\":%d,\"type_oid\":%u,\"value\":",
						 i ? "," : "", k->sk_attno, k->sk_strategy, k->sk_flags, type);
		sib_scan_key(out, k, type);
		appendStringInfoChar(out, '}');
	}
	appendStringInfoChar(out, ']');
}

static void
sib_scan(const char *event, IndexScanDesc scan, ScanKey keys, int nkeys,
		 ItemPointer tid, int direction, bool found)
{
	StringInfoData out;

	initStringInfo(&out);
	sib_scan_detail(&out, scan, keys, nkeys, tid, direction, found);
	sib_emit(event, scan->indexRelation, out.data);
	pfree(out.data);
}

/* Appends the text of the given heap columns of a fetched row as a JSON array. */
static void
sib_row_columns(StringInfo out, TupleTableSlot *slot, const AttrNumber *columns, int count)
{
	int			i;
	bool		isnull;
	Datum		value;

	appendStringInfoChar(out, '[');
	for (i = 0; i < count; i++)
	{
		if (i)
			appendStringInfoChar(out, ',');
		if (columns[i] <= 0)
		{
			appendStringInfoString(out, "null");
			continue;
		}
		value = slot_getattr(slot, columns[i], &isnull);
		sib_datum(out, TupleDescAttr(slot->tts_tupleDescriptor, columns[i] - 1)->atttypid,
				  value, isnull);
	}
	appendStringInfoChar(out, ']');
}

/* The columns of the table's primary key, in attribute order. */
static int
sib_primary_columns(Relation heap, AttrNumber *columns)
{
	Bitmapset  *primary;
	int			count = 0,
				member = -1;

	primary = RelationGetIndexAttrBitmap(heap, INDEX_ATTR_BITMAP_PRIMARY_KEY);
	while ((member = bms_next_member(primary, member)) >= 0 && count < INDEX_MAX_KEYS)
		columns[count++] = member + FirstLowInvalidHeapAttributeNumber;
	bms_free(primary);
	return count;
}

/* The per-row heap verdict of one index entry, with the visible row's identity. */
static void
sib_heap_visibility_detail(IndexScanDesc scan, TupleTableSlot *slot, bool found)
{
	StringInfoData out;
	AttrNumber	columns[INDEX_MAX_KEYS];
	int			count;

	initStringInfo(&out);
	sib_scan_detail(&out, scan, NULL, 0, found ? &slot->tts_tid : &scan->xs_heaptid, 0, found);
	if (found)
	{
		appendStringInfoString(&out, ",\"row\":{\"indexed\":");
		sib_row_columns(&out, slot, scan->indexRelation->rd_index->indkey.values,
						scan->indexRelation->rd_index->indnkeyatts);
		count = sib_primary_columns(scan->heapRelation, columns);
		appendStringInfoString(&out, ",\"pk\":");
		sib_row_columns(&out, slot, columns, count);
		appendStringInfoChar(&out, '}');
	}
	sib_emit("heap_visibility", scan->indexRelation, out.data);
	pfree(out.data);
}

/* ------------------------------------------------------------------------------------------ */
/* Access units                                                                               */
/* ------------------------------------------------------------------------------------------ */

/* Bitmap candidate TIDs of one unit, as block << 16 | offset; sorted and deduplicated once. */
typedef struct SibTids
{
	uint64	   *items;
	bool	   *resolved;
	int64		count;
	int64		capacity;
	int64		distinct;
	bool		sealed;
} SibTids;

typedef struct SibUnit
{
	void	   *scan;			/* hash key: the live IndexScanDesc, never dereferenced late */
	Oid			index_oid;
	NameData	index_name;
	void	   *heap;			/* the bitmap heap node the scan feeds, as of its latest binding */
	int			position;		/* the scan's position among that node's inputs */
	/*
	 * Both live with the scan and are siblings: resetting a context deletes its children. The unit
	 * context is reset when a unit starts or ends, the run context also when the value changes.
	 */
	MemoryContext unit_cxt;
	MemoryContext run_cxt;
	bool		active;
	bool		array_keys;
	bool		fetched_before_binding;
	bool		candidates_after_verdict;
	/* Visible-row identity: the scanning index's value and the table's single-column PK. */
	bool		identity_ready;
	bool		identity_ok;
	AttrNumber	value_column;
	AttrNumber	pk_column;
	bool		value_byval;
	bool		pk_byval;
	int16		value_len;
	int16		pk_len;
	FmgrInfo	value_output;
	FmgrInfo	pk_output;
	/* Counters, exactly as the ledger accumulates them from per-row events. */
	int64		calls;
	int64		found;
	int64		recheck;
	int64		heap_fetches;
	int64		heap_visible;
	int64		vm_visible;
	int64		bitmap_tids;
	bool		last_found;
	bool		forward;
	bool		backward;
	bool		bitmap_ended;
	bool		identity_missing;
	int64		rows_before_last_run;
	bool		has_last;
	bool		last_null;
	Datum		last_value;
	Datum	   *pks;
	bool	   *pk_nulls;
	int64		npks;
	int64		pk_capacity;
	StringInfoData runs;		/* completed runs as JSON [value, rows] pairs, array keys only */
	SibTids		tids;
} SibUnit;

typedef struct SibVerdictSet
{
	uint64		inputs;			/* bit i: input i's current unit produced the TID */
	int64		visible;
	int64		invisible;
} SibVerdictSet;

typedef enum SibCombination
{
	SIB_SINGLE,
	SIB_OR,
	SIB_AND,
	SIB_NESTED
} SibCombination;

typedef struct SibHeap
{
	void	   *node;			/* hash key: the BitmapHeapScanState */
	Oid			heap_oid;
	NameData	heap_name;
	int			ninputs;
	void	   *inputs[SIB_MAX_INPUTS];
	bool		overflow;
	SibCombination combination;
	/* A BitmapAnd's current cycle, as the ledger forms it: the inputs that started in it. */
	bool		cycle_open;
	bool		cycle_sealed;
	bool		cycle_reported;
	uint64		cycle_members;
	SibVerdictSet pending[SIB_PENDING_SETS];
	int			npending;
	int64		lossy_pages;
} SibHeap;

static HTAB *sib_units = NULL;
static HTAB *sib_heaps = NULL;

static void
sib_tables(void)
{
	HASHCTL		ctl;

	if (sib_units)
		return;
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(void *);
	ctl.entrysize = sizeof(SibUnit);
	sib_units = hash_create("SIBench units", 64, &ctl, HASH_ELEM | HASH_BLOBS);
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(void *);
	ctl.entrysize = sizeof(SibHeap);
	sib_heaps = hash_create("SIBench bitmap heaps", 16, &ctl, HASH_ELEM | HASH_BLOBS);
}

static SibUnit *
sib_unit_find(void *scan)
{
	if (!sib_units)
		return NULL;
	return (SibUnit *) hash_search(sib_units, &scan, HASH_FIND, NULL);
}

static SibHeap *
sib_heap_find(void *node)
{
	if (!sib_heaps || !node)
		return NULL;
	return (SibHeap *) hash_search(sib_heaps, &node, HASH_FIND, NULL);
}

/* The scan's entry, created on first sight; a new entry feeds no bitmap heap node. */
static SibUnit *
sib_unit_entry(IndexScanDesc scan)
{
	SibUnit    *unit;
	bool		found;
	void	   *key = scan;

	sib_tables();
	unit = (SibUnit *) hash_search(sib_units, &key, HASH_ENTER, &found);
	if (!found)
	{
		unit->index_oid = RelationGetRelid(scan->indexRelation);
		namestrcpy(&unit->index_name, RelationGetRelationName(scan->indexRelation));
		unit->heap = NULL;
		unit->position = -1;
		unit->unit_cxt = AllocSetContextCreate(TopMemoryContext, "SIBench unit",
											   ALLOCSET_SMALL_SIZES);
		unit->run_cxt = AllocSetContextCreate(TopMemoryContext, "SIBench run",
											  ALLOCSET_SMALL_SIZES);
		unit->active = false;
		unit->identity_ready = false;
	}
	return unit;
}

static void
sib_unit_remove(SibUnit *unit)
{
	void	   *key = unit->scan;

	MemoryContextDelete(unit->unit_cxt);
	MemoryContextDelete(unit->run_cxt);
	hash_search(sib_units, &key, HASH_REMOVE, NULL);
}

static void
sib_unit_start(SibUnit *unit, ScanKey keys, int nkeys)
{
	int			i;
	void	   *scan = unit->scan;
	Oid			index_oid = unit->index_oid;
	NameData	index_name = unit->index_name;
	void	   *heap = unit->heap;
	int			position = unit->position;
	MemoryContext unit_cxt = unit->unit_cxt,
				run_cxt = unit->run_cxt;
	bool		identity_ready = unit->identity_ready,
				identity_ok = unit->identity_ok;
	AttrNumber	value_column = unit->value_column,
				pk_column = unit->pk_column;
	bool		value_byval = unit->value_byval,
				pk_byval = unit->pk_byval;
	int16		value_len = unit->value_len,
				pk_len = unit->pk_len;
	FmgrInfo	value_output = unit->value_output,
				pk_output = unit->pk_output;

	MemoryContextReset(unit_cxt);
	MemoryContextReset(run_cxt);
	MemSet(unit, 0, sizeof(SibUnit));
	unit->scan = scan;
	unit->index_oid = index_oid;
	unit->index_name = index_name;
	unit->heap = heap;
	unit->position = position;
	unit->unit_cxt = unit_cxt;
	unit->run_cxt = run_cxt;
	unit->identity_ready = identity_ready;
	unit->identity_ok = identity_ok;
	unit->value_column = value_column;
	unit->pk_column = pk_column;
	unit->value_byval = value_byval;
	unit->pk_byval = pk_byval;
	unit->value_len = value_len;
	unit->pk_len = pk_len;
	unit->value_output = value_output;
	unit->pk_output = pk_output;
	unit->active = true;
	for (i = 0; i < nkeys; i++)
		if ((keys[i].sk_flags & SK_SEARCHARRAY) && !(keys[i].sk_flags & SK_ROW_HEADER))
			unit->array_keys = true;
}

/* Reports an index event for a scan with no unit; the ledger rejects it, as for per-row input. */
static SibUnit *
sib_unit_current(IndexScanDesc scan, const char *event)
{
	SibUnit    *unit = sib_unit_find(scan);
	char		detail[128];

	if (unit && unit->active)
		return unit;
	snprintf(detail, sizeof(detail), "\"scan\":\"%p\",\"orphan_event\":\"%s\"", scan, event);
	sib_emit("scan_orphan", scan->indexRelation, detail);
	return NULL;
}

static int
sib_tid_compare(const void *a, const void *b)
{
	uint64		x = *(const uint64 *) a,
				y = *(const uint64 *) b;

	return x < y ? -1 : x > y ? 1 : 0;
}

static void
sib_tids_seal(SibUnit *unit)
{
	SibTids    *tids = &unit->tids;
	int64		i,
				distinct = 0;

	if (tids->sealed)
		return;
	if (tids->count > 1)
		qsort(tids->items, tids->count, sizeof(uint64), sib_tid_compare);
	for (i = 0; i < tids->count; i++)
		if (distinct == 0 || tids->items[distinct - 1] != tids->items[i])
			tids->items[distinct++] = tids->items[i];
	tids->distinct = distinct;
	tids->resolved = distinct > 0
		? MemoryContextAllocHuge(unit->unit_cxt, distinct * sizeof(bool))
		: NULL;
	if (distinct > 0)
		memset(tids->resolved, 0, distinct * sizeof(bool));
	tids->sealed = true;
}

static int64
sib_tids_find(SibTids *tids, uint64 tid)
{
	int64		low = 0,
				high = tids->distinct - 1;

	while (low <= high)
	{
		int64		middle = low + (high - low) / 2;

		if (tids->items[middle] == tid)
			return middle;
		if (tids->items[middle] < tid)
			low = middle + 1;
		else
			high = middle - 1;
	}
	return -1;
}

static int64
sib_tids_unresolved(SibUnit *unit)
{
	int64		i,
				count = 0;

	sib_tids_seal(unit);
	for (i = 0; i < unit->tids.distinct; i++)
		if (!unit->tids.resolved[i])
			count++;
	return count;
}

/* ------------------------------------------------------------------------------------------ */
/* Bitmap heap nodes                                                                          */
/* ------------------------------------------------------------------------------------------ */

static void
sib_heap_flush(SibHeap *heap)
{
	StringInfoData out;
	int			i,
				bit;
	bool		first;

	if (heap->npending == 0 && heap->lossy_pages == 0)
		return;
	initStringInfo(&out);
	appendStringInfo(&out, "\"heap\":\"%p\",\"lossy_pages\":%lld,\"verdicts\":[",
					 heap->node, (long long) heap->lossy_pages);
	for (i = 0; i < heap->npending; i++)
	{
		SibVerdictSet *set = &heap->pending[i];

		appendStringInfo(&out, "%s{\"inputs\":[", i ? "," : "");
		first = true;
		for (bit = 0; bit < heap->ninputs; bit++)
			if (set->inputs & (UINT64CONST(1) << bit))
			{
				appendStringInfo(&out, "%s%d", first ? "" : ",", bit);
				first = false;
			}
		appendStringInfo(&out, "],\"visible\":%lld,\"invisible\":%lld}",
						 (long long) set->visible, (long long) set->invisible);
	}
	appendStringInfoChar(&out, ']');
	sib_emit_named("bitmap_verdicts", heap->heap_oid, NameStr(heap->heap_name), out.data);
	pfree(out.data);
	heap->npending = 0;
	heap->lossy_pages = 0;
}

static SibUnit *
sib_heap_input_unit(SibHeap *heap, int position)
{
	SibUnit    *unit = sib_unit_find(heap->inputs[position]);

	return unit && unit->active ? unit : NULL;
}

/*
 * Whether one candidate TID was produced, and is still unchecked, by every member of the current
 * BitmapAnd cycle: the heap scan stopped before checking it.
 */
static bool
sib_heap_unresolved_intersection(SibHeap *heap)
{
	SibUnit    *members[SIB_MAX_INPUTS];
	SibUnit    *smallest = NULL;
	int64		smallest_count = 0;
	int			n = 0,
				i,
				j;
	int64		k;

	for (i = 0; i < heap->ninputs; i++)
	{
		SibUnit    *unit;
		int64		count;

		if (!(heap->cycle_members & (UINT64CONST(1) << i)))
			continue;
		unit = sib_heap_input_unit(heap, i);
		if (!unit)
			continue;
		members[n++] = unit;
		count = sib_tids_unresolved(unit);
		if (!smallest || count < smallest_count)
		{
			smallest = unit;
			smallest_count = count;
		}
	}
	if (!smallest)
		return false;
	for (k = 0; k < smallest->tids.distinct; k++)
	{
		bool		everywhere = true;

		if (smallest->tids.resolved[k])
			continue;
		for (j = 0; j < n && everywhere; j++)
		{
			int64		at;

			if (members[j] == smallest)
				continue;
			at = sib_tids_find(&members[j]->tids, smallest->tids.items[k]);
			everywhere = at >= 0 && !members[j]->tids.resolved[at];
		}
		if (everywhere)
			return true;
	}
	return false;
}

/* ------------------------------------------------------------------------------------------ */
/* Unit summaries                                                                             */
/* ------------------------------------------------------------------------------------------ */

#define SIB_BOOL(b) ((b) ? "true" : "false")

/* Writes the unit's summary and ends it. Pending verdicts of its heap node are written first. */
static void
sib_unit_flush(SibUnit *unit)
{
	StringInfoData out;
	SibHeap    *heap;
	int64		i;
	bool		limited;

	if (!unit->active)
		return;
	heap = sib_heap_find(unit->heap);
	if (heap)
		sib_heap_flush(heap);
	sib_tids_seal(unit);
	initStringInfo(&out);
	appendStringInfo(&out,
					 "\"scan\":\"%p\",\"calls\":%lld,\"found\":%lld,\"last_found\":%s,\"forward\":%s,"
					 "\"backward\":%s,\"recheck\":%lld,\"heap_fetches\":%lld,\"heap_visible\":%lld,"
					 "\"vm_visible\":%lld,\"bitmap_tids\":%lld,\"bitmap_distinct\":%lld,"
					 "\"bitmap_unresolved\":%lld,\"bitmap_ended\":%s,\"identity_missing\":%s",
					 unit->scan, (long long) unit->calls, (long long) unit->found,
					 SIB_BOOL(unit->last_found), SIB_BOOL(unit->forward), SIB_BOOL(unit->backward),
					 (long long) unit->recheck, (long long) unit->heap_fetches,
					 (long long) unit->heap_visible, (long long) unit->vm_visible,
					 (long long) unit->bitmap_tids, (long long) unit->tids.distinct,
					 (long long) sib_tids_unresolved(unit), SIB_BOOL(unit->bitmap_ended),
					 SIB_BOOL(unit->identity_missing));
	if (unit->fetched_before_binding)
		appendStringInfoString(&out, ",\"fetched_before_binding\":true");
	if (unit->candidates_after_verdict)
		appendStringInfoString(&out, ",\"candidates_after_verdict\":true");
	/* The returned rows' identities matter only to a scan its consumer stopped early. */
	limited = unit->calls > 0 && unit->last_found;
	if (limited)
	{
		appendStringInfo(&out, ",\"rows_before_last_run\":%lld,\"runs\":[",
						 (long long) unit->rows_before_last_run);
		if (unit->runs.data)
			appendBinaryStringInfo(&out, unit->runs.data, unit->runs.len);
		appendStringInfoChar(&out, ']');
		if (unit->has_last)
		{
			appendStringInfoString(&out, ",\"last_value\":");
			sib_datum_with(&out, &unit->value_output, unit->last_value, unit->last_null);
			appendStringInfoString(&out, ",\"last_pks\":[");
			for (i = 0; i < unit->npks; i++)
			{
				if (i)
					appendStringInfoChar(&out, ',');
				sib_datum_with(&out, &unit->pk_output, unit->pks[i], unit->pk_nulls[i]);
			}
			appendStringInfoChar(&out, ']');
		}
	}
	if (heap && heap->combination == SIB_AND && heap->cycle_open && !heap->cycle_reported
		&& unit->position >= 0 && (heap->cycle_members & (UINT64CONST(1) << unit->position)))
	{
		appendStringInfo(&out, ",\"group_unresolved_intersection\":%s",
						 SIB_BOOL(sib_heap_unresolved_intersection(heap)));
		heap->cycle_reported = true;
	}
	sib_emit_named("scan_summary", unit->index_oid, NameStr(unit->index_name), out.data);
	pfree(out.data);
	unit->active = false;
	MemoryContextReset(unit->unit_cxt);
	MemoryContextReset(unit->run_cxt);
	unit->tids.items = NULL;
	unit->tids.resolved = NULL;
	unit->pks = NULL;
	unit->pk_nulls = NULL;
	unit->runs.data = NULL;
}

/* Everything still open: before an abort settles its scans, and at backend exit. */
static void
sib_flush_all(void)
{
	HASH_SEQ_STATUS status;
	SibHeap    *heap;
	SibUnit    *unit;

	if (!sib_units || !sib_enabled())
		return;
	hash_seq_init(&status, sib_heaps);
	while ((heap = (SibHeap *) hash_seq_search(&status)) != NULL)
		sib_heap_flush(heap);
	hash_seq_init(&status, sib_units);
	while ((unit = (SibUnit *) hash_seq_search(&status)) != NULL)
		sib_unit_flush(unit);
}

/* ------------------------------------------------------------------------------------------ */
/* Index scan hooks                                                                           */
/* ------------------------------------------------------------------------------------------ */

void
sib_plan_scan(const char *kind, int plan_node_id, IndexScanDesc scan)
{
	char		detail[160];
	SibUnit    *unit;

	if (!scan || !sib_relation(scan->indexRelation))
		return;
	unit = sib_unit_entry(scan);
	/* A reused descriptor address is a new scan: the ledger rejects a live one. */
	sib_unit_flush(unit);
	unit->heap = NULL;
	unit->position = -1;
	unit->identity_ready = false;
	snprintf(detail, sizeof(detail), "\"scan\":\"%p\",\"plan_node\":%d,\"kind\":\"%s\"",
			 (void *) scan, plan_node_id, kind);
	sib_emit("plan_scan", scan->indexRelation, detail);
}

/* A BitmapAnd input that runs again starts the next cycle, as in the ledger. */
static void
sib_cycle_join(SibHeap *heap, int position)
{
	uint64		bit = UINT64CONST(1) << position;

	if (heap->combination != SIB_AND)
		return;
	if (heap->cycle_open && (heap->cycle_members & bit))
		heap->cycle_sealed = true;
	if (!heap->cycle_open || heap->cycle_sealed)
	{
		heap->cycle_open = true;
		heap->cycle_sealed = false;
		heap->cycle_reported = false;
		heap->cycle_members = 0;
	}
	heap->cycle_members |= bit;
}

void
sib_index_rescan(IndexScanDesc scan, ScanKey keys, int nkeys)
{
	SibUnit    *unit;
	SibHeap    *heap;

	if (!sib_relation(scan->indexRelation))
		return;
	unit = sib_unit_entry(scan);
	heap = sib_heap_find(unit->heap);
	if (heap)
		sib_heap_flush(heap);
	sib_unit_flush(unit);
	sib_scan("scan_rescan", scan, keys, nkeys, NULL, 0, false);
	if (heap && unit->position >= 0)
		sib_cycle_join(heap, unit->position);
	sib_unit_start(unit, keys, nkeys);
}

void
sib_index_end(IndexScanDesc scan)
{
	SibUnit    *unit;
	SibHeap    *heap;

	if (!sib_relation(scan->indexRelation))
		return;
	unit = sib_unit_find(scan);
	if (unit)
	{
		heap = sib_heap_find(unit->heap);
		if (heap)
			sib_heap_flush(heap);
		sib_unit_flush(unit);
		/* An ended scan never joins another cycle of its BitmapAnd. */
		if (heap && heap->cycle_open)
			heap->cycle_sealed = true;
	}
	sib_scan("scan_end", scan, NULL, 0, NULL, 0, false);
	if (unit)
		sib_unit_remove(unit);
}

void
sib_index_next(IndexScanDesc scan, ScanDirection direction, bool found)
{
	SibUnit    *unit;

	if (!sib_relation(scan->indexRelation))
		return;
	if (sib_detail())
		sib_scan("scan_next", scan, NULL, 0, found ? &scan->xs_heaptid : NULL, direction, found);
	unit = sib_unit_current(scan, "scan_next");
	if (!unit)
		return;
	unit->calls++;
	unit->last_found = found;
	if (direction > 0)
		unit->forward = true;
	else if (direction < 0)
		unit->backward = true;
	if (found)
	{
		unit->found++;
		if (scan->xs_recheck)
			unit->recheck++;
	}
}

/* Resolves the identity columns once per scan; anything but one column each has no identity. */
static void
sib_identity_prepare(SibUnit *unit, IndexScanDesc scan, TupleTableSlot *slot)
{
	AttrNumber	columns[INDEX_MAX_KEYS];
	Form_pg_attribute attribute;
	Oid			output;
	bool		variable;

	unit->identity_ready = true;
	unit->identity_ok = scan->indexRelation->rd_index->indnkeyatts == 1
		&& sib_primary_columns(scan->heapRelation, columns) == 1;
	if (!unit->identity_ok)
		return;
	unit->value_column = scan->indexRelation->rd_index->indkey.values[0];
	unit->pk_column = columns[0];
	if (unit->value_column > 0)
	{
		attribute = TupleDescAttr(slot->tts_tupleDescriptor, unit->value_column - 1);
		unit->value_byval = attribute->attbyval;
		unit->value_len = attribute->attlen;
		getTypeOutputInfo(attribute->atttypid, &output, &variable);
		fmgr_info_cxt(output, &unit->value_output, TopMemoryContext);
	}
	attribute = TupleDescAttr(slot->tts_tupleDescriptor, unit->pk_column - 1);
	unit->pk_byval = attribute->attbyval;
	unit->pk_len = attribute->attlen;
	getTypeOutputInfo(attribute->atttypid, &output, &variable);
	fmgr_info_cxt(output, &unit->pk_output, TopMemoryContext);
}

/* A private, fully detoasted copy: it is written after the scan, possibly outside a transaction. */
static Datum
sib_copy(Datum value, bool byval, int16 length, MemoryContext context)
{
	MemoryContext previous;
	Datum		copy;

	if (byval)
		return value;
	previous = MemoryContextSwitchTo(context);
	if (length == -1)
		copy = PointerGetDatum(PG_DETOAST_DATUM_COPY(value));
	else
		copy = datumCopy(value, false, length);
	MemoryContextSwitchTo(previous);
	return copy;
}

static bool
sib_same_value(SibUnit *unit, Datum value, bool isnull)
{
	if (isnull || unit->last_null)
		return isnull && unit->last_null;
	return datum_image_eq(unit->last_value, value, unit->value_byval, unit->value_len);
}

/* One visible row, in scan order: extends the last run of equal values or starts the next. */
static void
sib_identity(SibUnit *unit, IndexScanDesc scan, TupleTableSlot *slot)
{
	Datum		value = (Datum) 0,
				pk;
	bool		value_null = true,
				pk_null;
	MemoryContext previous;

	if (!unit->identity_ready)
		sib_identity_prepare(unit, scan, slot);
	if (!unit->identity_ok)
	{
		unit->identity_missing = true;
		return;
	}
	if (unit->value_column > 0)
		value = slot_getattr(slot, unit->value_column, &value_null);
	pk = slot_getattr(slot, unit->pk_column, &pk_null);
	if (unit->has_last && !sib_same_value(unit, value, value_null))
	{
		if (unit->array_keys)
		{
			previous = MemoryContextSwitchTo(unit->unit_cxt);
			if (!unit->runs.data)
				initStringInfo(&unit->runs);
			else
				appendStringInfoChar(&unit->runs, ',');
			appendStringInfoChar(&unit->runs, '[');
			sib_datum_with(&unit->runs, &unit->value_output, unit->last_value, unit->last_null);
			appendStringInfo(&unit->runs, ",%lld]", (long long) unit->npks);
			MemoryContextSwitchTo(previous);
		}
		unit->rows_before_last_run += unit->npks;
		MemoryContextReset(unit->run_cxt);
		unit->has_last = false;
		unit->pks = NULL;
		unit->pk_nulls = NULL;
		unit->npks = 0;
		unit->pk_capacity = 0;
	}
	if (!unit->has_last)
	{
		unit->has_last = true;
		unit->last_null = value_null;
		unit->last_value = value_null ? (Datum) 0
			: sib_copy(value, unit->value_byval, unit->value_len, unit->run_cxt);
	}
	if (unit->npks == unit->pk_capacity)
	{
		int64		capacity = unit->pk_capacity ? unit->pk_capacity * 2 : 16;

		if (unit->pks)
		{
			unit->pks = repalloc_huge(unit->pks, capacity * sizeof(Datum));
			unit->pk_nulls = repalloc_huge(unit->pk_nulls, capacity * sizeof(bool));
		}
		else
		{
			unit->pks = MemoryContextAllocHuge(unit->run_cxt, capacity * sizeof(Datum));
			unit->pk_nulls = MemoryContextAllocHuge(unit->run_cxt, capacity * sizeof(bool));
		}
		unit->pk_capacity = capacity;
	}
	unit->pk_nulls[unit->npks] = pk_null;
	unit->pks[unit->npks++] = pk_null ? (Datum) 0
		: sib_copy(pk, unit->pk_byval, unit->pk_len, unit->run_cxt);
}

/*
 * The heap verdict of one index entry. A visible row also carries its identity, the values of the
 * scanning index's key columns and of the table's primary key, read from the visible heap tuple:
 * a scan its consumer stopped early returned exactly these rows, and their primary keys are the
 * only identity a KV index shares with PostgreSQL (heap TIDs have no counterpart).
 */
void
sib_heap_visibility(IndexScanDesc scan, TupleTableSlot *slot, bool found)
{
	SibUnit    *unit;

	if (!sib_relation(scan->indexRelation))
		return;
	if (sib_detail())
		sib_heap_visibility_detail(scan, slot, found);
	unit = sib_unit_current(scan, "heap_visibility");
	if (!unit)
		return;
	unit->heap_fetches++;
	if (found)
	{
		unit->heap_visible++;
		sib_identity(unit, scan, slot);
	}
}

/* An all-visible index-only row is returned without reading the heap: it has no identity. */
void
sib_vm_visible(IndexScanDesc scan, ItemPointer tid)
{
	SibUnit    *unit;

	if (!sib_relation(scan->indexRelation))
		return;
	if (sib_detail())
		sib_scan("vm_visible", scan, NULL, 0, tid, 0, true);
	unit = sib_unit_current(scan, "vm_visible");
	if (!unit)
		return;
	unit->vm_visible++;
	unit->identity_missing = true;
}

/* ------------------------------------------------------------------------------------------ */
/* Bitmap hooks                                                                               */
/* ------------------------------------------------------------------------------------------ */

void
sib_bitmap_candidates(IndexScanDesc scan, const ItemPointer tids, int ntids)
{
	SibUnit    *unit;
	SibTids    *set;
	int			i;

	if (!scan || !sib_relation(scan->indexRelation))
		return;
	if (sib_detail())
		for (i = 0; i < ntids; i++)
			sib_scan("bitmap_candidate", scan, NULL, 0, (ItemPointer) (tids + i), 0, true);
	unit = sib_unit_current(scan, "bitmap_candidate");
	if (!unit)
		return;
	set = &unit->tids;
	if (set->sealed)
	{
		unit->candidates_after_verdict = true;
		return;
	}
	if (set->count + ntids > set->capacity)
	{
		int64		capacity = Max(set->capacity * 2, set->count + ntids);

		capacity = Max(capacity, 1024);
		set->items = set->items
			? repalloc_huge(set->items, capacity * sizeof(uint64))
			: MemoryContextAllocHuge(unit->unit_cxt, capacity * sizeof(uint64));
		set->capacity = capacity;
	}
	for (i = 0; i < ntids; i++)
		set->items[set->count++] =
			((uint64) ItemPointerGetBlockNumber(tids + i) << 16) | ItemPointerGetOffsetNumber(tids + i);
	unit->bitmap_tids += ntids;
}

void
sib_bitmap_end(IndexScanDesc scan)
{
	SibUnit    *unit;

	if (!sib_relation(scan->indexRelation))
		return;
	if (sib_detail())
		sib_scan("bitmap_end", scan, NULL, 0, NULL, 0, false);
	unit = sib_unit_current(scan, "bitmap_end");
	if (!unit)
		return;
	unit->bitmap_ended = true;
	sib_tids_seal(unit);
}

/* Collects the index scans that feed a bitmap: one BitmapIndexScan, or BitmapAnd/Or inputs. */
static void
sib_bitmap_inputs(PlanState *input, StringInfo out, SibHeap *heap)
{
	int			i;

	if (IsA(input, BitmapIndexScanState))
	{
		void	   *scan = ((BitmapIndexScanState *) input)->biss_ScanDesc;

		appendStringInfo(out, "%s\"%p\"", out->data[out->len - 1] == '[' ? "" : ",", scan);
		if (heap->ninputs < SIB_MAX_INPUTS)
			heap->inputs[heap->ninputs++] = scan;
		else
			heap->overflow = true;
	}
	else if (IsA(input, BitmapOrState))
		for (i = 0; i < ((BitmapOrState *) input)->nplans; i++)
			sib_bitmap_inputs(((BitmapOrState *) input)->bitmapplans[i], out, heap);
	else if (IsA(input, BitmapAndState))
		for (i = 0; i < ((BitmapAndState *) input)->nplans; i++)
			sib_bitmap_inputs(((BitmapAndState *) input)->bitmapplans[i], out, heap);
}

/*
 * The combination of the index scans as a tree: a scan identity, or {"and":[...]} / {"or":[...]}
 * over subtrees. A flat input list cannot tell an intersection of a union from a union of scans.
 */
static void
sib_bitmap_tree(PlanState *input, StringInfo out)
{
	int			i,
				count;
	PlanState **plans;

	if (IsA(input, BitmapIndexScanState))
	{
		appendStringInfo(out, "\"%p\"", (void *) ((BitmapIndexScanState *) input)->biss_ScanDesc);
		return;
	}
	if (IsA(input, BitmapOrState))
	{
		appendStringInfoString(out, "{\"or\":[");
		count = ((BitmapOrState *) input)->nplans;
		plans = ((BitmapOrState *) input)->bitmapplans;
	}
	else
	{
		appendStringInfoString(out, "{\"and\":[");
		count = ((BitmapAndState *) input)->nplans;
		plans = ((BitmapAndState *) input)->bitmapplans;
	}
	for (i = 0; i < count; i++)
	{
		if (i)
			appendStringInfoChar(out, ',');
		sib_bitmap_tree(plans[i], out);
	}
	appendStringInfoString(out, "]}");
}

/* SINGLE, flat OR, flat AND, or a nesting of the two, exactly as the ledger reads the tree. */
static SibCombination
sib_bitmap_combination(PlanState *input)
{
	int			i,
				count;
	PlanState **plans;

	if (IsA(input, BitmapIndexScanState))
		return SIB_SINGLE;
	if (IsA(input, BitmapOrState))
	{
		count = ((BitmapOrState *) input)->nplans;
		plans = ((BitmapOrState *) input)->bitmapplans;
	}
	else
	{
		count = ((BitmapAndState *) input)->nplans;
		plans = ((BitmapAndState *) input)->bitmapplans;
	}
	for (i = 0; i < count; i++)
		if (!IsA(plans[i], BitmapIndexScanState))
			return SIB_NESTED;
	return IsA(input, BitmapOrState) ? SIB_OR : SIB_AND;
}

/* Binds a bitmap heap node to the index scans whose candidates it checks. */
void
sib_bitmap_binding(BitmapHeapScanState *node)
{
	StringInfoData out;
	SibHeap    *heap;
	bool		found;
	void	   *key = node;
	int			i;

	if (!sib_relation(node->ss.ss_currentRelation))
		return;
	sib_tables();
	heap = (SibHeap *) hash_search(sib_heaps, &key, HASH_ENTER, &found);
	if (found)
		sib_heap_flush(heap);
	heap->heap_oid = RelationGetRelid(node->ss.ss_currentRelation);
	namestrcpy(&heap->heap_name, RelationGetRelationName(node->ss.ss_currentRelation));
	heap->ninputs = 0;
	heap->overflow = false;
	heap->npending = 0;
	heap->lossy_pages = 0;
	heap->cycle_open = false;
	heap->cycle_sealed = false;
	heap->cycle_reported = false;
	heap->cycle_members = 0;
	heap->combination = sib_bitmap_combination(outerPlanState(node));
	initStringInfo(&out);
	appendStringInfo(&out, "\"heap\":\"%p\",\"plan_node\":%d,\"tree\":",
					 (void *) node, node->ss.ps.plan->plan_node_id);
	sib_bitmap_tree(outerPlanState(node), &out);
	appendStringInfoString(&out, ",\"scans\":[");
	sib_bitmap_inputs(outerPlanState(node), &out, heap);
	appendStringInfoChar(&out, ']');
	if (heap->overflow)
		appendStringInfo(&out, ",\"inputs_over_limit\":%d", SIB_MAX_INPUTS);
	sib_emit("plan_bitmap_heap", node->ss.ss_currentRelation, out.data);
	pfree(out.data);
	/* PostgreSQL passes constant keys to the inputs before this node exists: their first cycle. */
	for (i = 0; i < heap->ninputs; i++)
	{
		SibUnit    *unit = sib_unit_find(heap->inputs[i]);

		if (!unit)
			continue;
		unit->heap = node;
		unit->position = i;
		if (!unit->active)
			continue;
		if (unit->calls > 0 || unit->bitmap_tids > 0 || unit->bitmap_ended)
			unit->fetched_before_binding = true;
		if (heap->combination == SIB_AND)
		{
			heap->cycle_open = true;
			heap->cycle_members |= UINT64CONST(1) << i;
		}
	}
}

/* Adds one verdict to the node's pending sets, claiming the TID in every input that has it. */
static void
sib_heap_verdict(SibHeap *heap, uint64 tid, bool found)
{
	uint64		inputs = 0;
	int			i;
	SibVerdictSet *set = NULL;

	for (i = 0; i < heap->ninputs; i++)
	{
		SibUnit    *unit = sib_heap_input_unit(heap, i);
		int64		at;

		if (!unit)
			continue;
		sib_tids_seal(unit);
		at = sib_tids_find(&unit->tids, tid);
		if (at < 0 || unit->tids.resolved[at])
			continue;
		unit->tids.resolved[at] = true;
		inputs |= UINT64CONST(1) << i;
	}
	for (i = 0; i < heap->npending; i++)
		if (heap->pending[i].inputs == inputs)
		{
			set = &heap->pending[i];
			break;
		}
	if (!set)
	{
		/* Writing early is always exact: the inputs' units have not changed. */
		if (heap->npending == SIB_PENDING_SETS)
			sib_heap_flush(heap);
		set = &heap->pending[heap->npending++];
		set->inputs = inputs;
		set->visible = 0;
		set->invisible = 0;
	}
	if (found)
		set->visible++;
	else
		set->invisible++;
}

/* Heap visibility of one bitmap candidate TID, for every index scan that contributed it. */
void
sib_bitmap_verdict(Relation relation, void *node, ItemPointer tid, bool found)
{
	SibHeap    *heap;

	if (!sib_relation(relation))
		return;
	if (sib_detail())
	{
		char		detail[160];

		snprintf(detail, sizeof(detail), "\"heap\":\"%p\",\"tid\":\"(%u,%u)\",\"found\":%s", node,
				 ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid), found ? "true" : "false");
		sib_emit("bitmap_heap_candidate", relation, detail);
	}
	heap = sib_heap_find(node);
	if (heap)
		sib_heap_verdict(heap,
						 ((uint64) ItemPointerGetBlockNumber(tid) << 16) | ItemPointerGetOffsetNumber(tid),
						 found);
	else
		sib_emit("bitmap_heap_unbound", relation, NULL);
}

/* All-visible pages are not fetched; every exact candidate on them is visible. */
void
sib_bitmap_skipped(BitmapHeapScanState *node, TBMIterateResult *tbmres)
{
	int			i;
	ItemPointerData tid;

	for (i = 0; i < tbmres->ntuples; i++)
	{
		ItemPointerSet(&tid, tbmres->blockno, tbmres->offsets[i]);
		sib_bitmap_verdict(node->ss.ss_currentRelation, node, &tid, true);
	}
}

/* A lossy page has no candidate TIDs to attribute to an index. */
void
sib_bitmap_lossy(Relation relation, void *node)
{
	SibHeap    *heap;

	if (!sib_relation(relation))
		return;
	if (sib_detail())
	{
		char		detail[64];

		snprintf(detail, sizeof(detail), "\"heap\":\"%p\"", node);
		sib_emit("bitmap_heap_lossy", relation, detail);
	}
	heap = sib_heap_find(node);
	if (heap)
		heap->lossy_pages++;
	else
		sib_emit("bitmap_heap_unbound", relation, NULL);
}

/* ------------------------------------------------------------------------------------------ */
/* Writes                                                                                     */
/* ------------------------------------------------------------------------------------------ */

void
sib_index_insert(Relation relation, Datum *values, bool *isnull, ItemPointer tid, int unique)
{
	StringInfoData out;
	int			i;

	if (!sib_relation(relation))
		return;
	initStringInfo(&out);
	appendStringInfo(&out, "\"heap_oid\":%u,\"tid\":\"(%u,%u)\",\"unique_check\":%d,\"values\":[",
					 relation->rd_index->indrelid, ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid), unique);
	for (i = 0; i < RelationGetDescr(relation)->natts; i++)
	{
		if (i)
			appendStringInfoChar(&out, ',');
		sib_datum(&out, TupleDescAttr(RelationGetDescr(relation), i)->atttypid, values[i], isnull[i]);
	}
	appendStringInfoChar(&out, ']');
	sib_emit("index_insert_attempt", relation, out.data);
	pfree(out.data);
}

void
sib_index_build_start(Relation index)
{
	if (sib_relation(index))
		sib_emit("index_build_start", index, NULL);
}

void
sib_index_build_end(Relation index, double heap_rows, double index_rows)
{
	char		detail[160];

	if (!sib_relation(index))
		return;
	snprintf(detail, sizeof(detail), "\"heap_rows\":%.0f,\"index_rows\":%.0f", heap_rows, index_rows);
	sib_emit("index_build_end", index, detail);
}

/* ------------------------------------------------------------------------------------------ */
/* Executor, results and transactions                                                         */
/* ------------------------------------------------------------------------------------------ */

void
sib_executor_start(QueryDesc *queryDesc)
{
	StringInfoData detail;

	if (!sib_enabled())
		return;
	initStringInfo(&detail);
	appendStringInfoString(&detail, "\"sql\":");
	escape_json(&detail, queryDesc->sourceText ? queryDesc->sourceText : "");
	/* Core query jumbling (compute_query_id = on): constants do not change the identity. */
	appendStringInfo(&detail, ",\"query_id\":%lld", (long long) queryDesc->plannedstmt->queryId);
	sib_emit("executor_start", NULL, detail.data);
	pfree(detail.data);
}

void
sib_executor_end(uint64 processed)
{
	char		detail[128];

	if (!sib_enabled())
		return;
	snprintf(detail, sizeof(detail), "\"processed\":%llu", (unsigned long long) processed);
	sib_emit("executor_end", NULL, detail);
}

/*
 * Result rows are recorded as one 64-bit FNV-1a hash per row over canonical column text, which the
 * capture driver computes the same way from what the client read. Canonical text removes only the
 * representation differences of a binary JDBC transfer: float bits, numeric without trailing
 * fraction zeros, timestamps with six fraction digits, char without its padding.
 */
#define SIB_FNV_OFFSET UINT64CONST(14695981039346656037)
#define SIB_FNV_PRIME UINT64CONST(1099511628211)

static uint64
sib_fnv(uint64 hash, const char *data, size_t length)
{
	size_t		i;

	for (i = 0; i < length; i++)
	{
		hash ^= (unsigned char) data[i];
		hash *= SIB_FNV_PRIME;
	}
	return hash;
}

static uint64
sib_canonical(uint64 hash, Oid type, FmgrInfo *output, Datum value)
{
	char		buffer[64];
	char	   *text;
	size_t		length;

	switch (type)
	{
		case FLOAT4OID:
			{
				float4		f = DatumGetFloat4(value);
				uint32		bits;

				if (isnan(f))
					bits = 0x7fc00000;
				else
					memcpy(&bits, &f, sizeof(bits));
				snprintf(buffer, sizeof(buffer), "%08x", bits);
				return sib_fnv(hash, buffer, strlen(buffer));
			}
		case FLOAT8OID:
			{
				float8		f = DatumGetFloat8(value);
				uint64		bits;

				if (isnan(f))
					bits = UINT64CONST(0x7ff8000000000000);
				else
					memcpy(&bits, &f, sizeof(bits));
				snprintf(buffer, sizeof(buffer), "%016llx", (unsigned long long) bits);
				return sib_fnv(hash, buffer, strlen(buffer));
			}
		case TIMESTAMPOID:
			{
				Timestamp	ts = DatumGetTimestamp(value);
				struct pg_tm tm;
				fsec_t		fsec;

				if (!TIMESTAMP_NOT_FINITE(ts) && timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) == 0
					&& tm.tm_year > 0 && tm.tm_year <= 9999)
				{
					snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%06d",
							 tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
							 (int) fsec);
					return sib_fnv(hash, buffer, strlen(buffer));
				}
				break;
			}
		default:
			break;
	}
	text = OutputFunctionCall(output, value);
	length = strlen(text);
	if (type == BPCHAROID)
		while (length > 0 && text[length - 1] == ' ')
			length--;
	else if (type == NUMERICOID && strchr(text, '.'))
	{
		while (length > 0 && text[length - 1] == '0')
			length--;
		if (length > 0 && text[length - 1] == '.')
			length--;
	}
	if (type == NUMERICOID && length == 2 && strncmp(text, "-0", 2) == 0)
		hash = sib_fnv(hash, "0", 1);
	else
		hash = sib_fnv(hash, text, length);
	pfree(text);
	return hash;
}

typedef struct SibReceiver
{
	DestReceiver pub;
	DestReceiver *original;
	int			natts;
	Oid		   *types;
	FmgrInfo   *outputs;
	uint64		returned;
	StringInfoData hashes;
} SibReceiver;

static void
sib_prepare_columns(SibReceiver *self, TupleDesc typeinfo)
{
	int			i;
	Oid			output;
	bool		variable;

	self->natts = typeinfo->natts;
	self->types = palloc(sizeof(Oid) * Max(1, typeinfo->natts));
	self->outputs = palloc(sizeof(FmgrInfo) * Max(1, typeinfo->natts));
	for (i = 0; i < typeinfo->natts; i++)
	{
		self->types[i] = TupleDescAttr(typeinfo, i)->atttypid;
		getTypeOutputInfo(self->types[i], &output, &variable);
		fmgr_info(output, &self->outputs[i]);
	}
}

static bool
sib_receive(TupleTableSlot *slot, DestReceiver *receiver)
{
	SibReceiver *self = (SibReceiver *) receiver;
	uint64		hash = SIB_FNV_OFFSET;
	int			i;

	slot_getallattrs(slot);
	if (sib_detail())
	{
		StringInfoData out;

		initStringInfo(&out);
		appendStringInfoString(&out, "\"values\":[");
		for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			if (i)
				appendStringInfoChar(&out, ',');
			sib_datum(&out, TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid,
					  slot->tts_values[i], slot->tts_isnull[i]);
		}
		appendStringInfoChar(&out, ']');
		sib_emit("result_row", NULL, out.data);
		pfree(out.data);
	}
	if (self->natts != slot->tts_tupleDescriptor->natts)
		sib_prepare_columns(self, slot->tts_tupleDescriptor);
	for (i = 0; i < self->natts; i++)
	{
		if (slot->tts_isnull[i])
			hash = sib_fnv(hash, "N", 1);
		else
		{
			hash = sib_fnv(hash, "V", 1);
			hash = sib_canonical(hash, self->types[i], &self->outputs[i], slot->tts_values[i]);
		}
		hash = sib_fnv(hash, "\x1f", 1);
	}
	self->returned++;
	if (self->returned > 1)
		appendStringInfoChar(&self->hashes, ',');
	appendStringInfoChar(&self->hashes, '"');
	sib_append_hex16(&self->hashes, hash);
	appendStringInfoChar(&self->hashes, '"');
	return self->original->receiveSlot(slot, self->original);
}

static void
sib_start(DestReceiver *receiver, int operation, TupleDesc typeinfo)
{
	SibReceiver *self = (SibReceiver *) receiver;

	sib_prepare_columns(self, typeinfo);
	self->original->rStartup(self->original, operation, typeinfo);
}

static void
sib_shutdown(DestReceiver *receiver)
{
	SibReceiver *self = (SibReceiver *) receiver;

	self->original->rShutdown(self->original);
}

static void
sib_destroy(DestReceiver *receiver)
{
	SibReceiver *self = (SibReceiver *) receiver;

	self->original->rDestroy(self->original);
	pfree(self);
}

DestReceiver *
sib_wrap_receiver(DestReceiver *original)
{
	SibReceiver *receiver;

	if (!sib_enabled())
		return original;
	receiver = palloc0(sizeof(SibReceiver));
	receiver->original = original;
	receiver->pub.receiveSlot = sib_receive;
	receiver->pub.rStartup = sib_start;
	receiver->pub.rShutdown = sib_shutdown;
	receiver->pub.rDestroy = sib_destroy;
	receiver->pub.mydest = original->mydest;
	initStringInfo(&receiver->hashes);
	return (DestReceiver *) receiver;
}

/* The rows this execution returned, as a count and their hashes in order. */
void
sib_executor_run_end(DestReceiver *dest, uint64 processed)
{
	StringInfoData detail;

	if (!sib_enabled())
		return;
	initStringInfo(&detail);
	appendStringInfo(&detail, "\"processed\":%llu", (unsigned long long) processed);
	if (dest && dest->receiveSlot == sib_receive)
	{
		SibReceiver *self = (SibReceiver *) dest;

		appendStringInfo(&detail, ",\"returned\":%llu,\"row_hashes\":[",
						 (unsigned long long) self->returned);
		appendBinaryStringInfo(&detail, self->hashes.data, self->hashes.len);
		appendStringInfoChar(&detail, ']');
		self->returned = 0;
		resetStringInfo(&self->hashes);
	}
	sib_emit("executor_run_end", NULL, detail.data);
	pfree(detail.data);
}

/*
 * Where a finishing transaction sits in PostgreSQL's transaction completion count, read after
 * ProcArrayEndTransaction. A transaction with an XID incremented the count under ProcArrayLock and
 * recorded the exact value on its PGPROC; reading the shared count instead would race with later
 * completions. A transaction without an XID changes no snapshot and does not increment it, so it
 * sits after every completion it could have observed: the current count.
 */
static uint64
sib_completion(void)
{
	if (TransactionIdIsValid(GetTopTransactionIdIfAny()))
		return MyProc->sibenchCompletion;
	return ShmemVariableCache->xactCompletionCount;
}

void
sib_transaction_commit(void)
{
	char		detail[64];

	if (!sib_enabled())
		return;
	snprintf(detail, sizeof(detail), "\"csn\":%llu", (unsigned long long) sib_completion());
	sib_emit("transaction_commit", NULL, detail);
}

/* A failed statement never ends its scans: their units end with the abort, after their summaries. */
void
sib_transaction_abort(void)
{
	char		detail[64];
	HASH_SEQ_STATUS status;
	SibUnit    *unit;
	SibHeap    *heap;

	if (!sib_enabled())
		return;
	sib_flush_all();
	if (sib_units)
	{
		hash_seq_init(&status, sib_units);
		while ((unit = (SibUnit *) hash_seq_search(&status)) != NULL)
			sib_unit_remove(unit);
		/* No scan runs again after an abort, so no BitmapAnd cycle can grow. */
		hash_seq_init(&status, sib_heaps);
		while ((heap = (SibHeap *) hash_seq_search(&status)) != NULL)
			if (heap->cycle_open)
				heap->cycle_sealed = true;
	}
	snprintf(detail, sizeof(detail), "\"csn\":%llu", (unsigned long long) sib_completion());
	sib_emit("transaction_abort", NULL, detail);
}
