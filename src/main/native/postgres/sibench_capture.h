/* PostgreSQL 14.24 capture instrumentation. Included once per backend translation unit. */
#ifndef SIBENCH_CAPTURE_H
#define SIBENCH_CAPTURE_H
#include "access/genam.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include <stdio.h>
#include <unistd.h>

extern char *application_name;
extern IndexScanDesc sibench_bitmap_scan;
/* The bitmap heap scan node whose page heapam is checking, while it checks it. */
extern void *sibench_bitmap_heap_node;
/*
 * Per-backend request identity, shared by every translation unit that includes this header.
 * sibench_request counts top-level executions (the first executor_start outside any executor);
 * sibench_depth is the number of executors currently active, so SPI/trigger queries are nested
 * under their parent request and planner-time probes are visible as depth 0.
 */
extern int sibench_depth;
extern uint64 sibench_request;
/* Completion counter of the last snapshot this backend built, kept because a copied or imported
 * snapshot loses the field GetSnapshotData() set. */
extern uint64 sibench_snapshot_csn;

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
    if (TransactionIdIsValid(GetTopTransactionIdIfAny())) return MyProc->sibenchCompletion;
    return ShmemVariableCache->xactCompletionCount;
}

static bool
sib_enabled(void)
{
    return application_name && strncmp(application_name, "sibench:", 8) == 0;
}

static bool
sib_relation(Relation relation)
{
    return sib_enabled() && relation && RelationGetRelid(relation) >= FirstNormalObjectId;
}

static void
sib_emit(const char *event, Relation relation, const char *detail)
{
    static FILE *stream = NULL;
    StringInfoData message;
    char path[MAXPGPATH];
    const char *directory;
    bool executor_end, transaction_abort;
    if (!sib_enabled()) return;
    /* Counters advance even when no capture directory is configured. */
    if (strcmp(event, "executor_start") == 0) {
        if (sibench_depth == 0) sibench_request++;
        sibench_depth++;
    }
    executor_end = strcmp(event, "executor_end") == 0;
    transaction_abort = strcmp(event, "transaction_abort") == 0;
    directory = getenv("SIBENCH_CAPTURE_DIR");
    if (directory && directory[0]) {
        if (!stream) {
            snprintf(path, sizeof(path), "%s/pg-%d.jsonl", directory, MyProcPid);
            stream = fopen(path, "a");
            if (!stream) elog(ERROR, "SIBench cannot open trace file %s", path);
            setvbuf(stream, NULL, _IOLBF, 65536);
        }
        initStringInfo(&message);
        appendStringInfo(&message, "{\"event\":\"%s\",\"pid\":%d,\"xid\":%u,\"cid\":%u,\"req\":%llu,\"depth\":%d,\"application\":",
                         event, MyProcPid, GetTopTransactionIdIfAny(), GetCurrentCommandId(false),
                         (unsigned long long) sibench_request, sibench_depth);
        escape_json(&message, application_name);
        if (relation) {
            appendStringInfo(&message, ",\"relation_oid\":%u,\"relation\":", RelationGetRelid(relation));
            escape_json(&message, RelationGetRelationName(relation));
        }
        if (detail && detail[0]) appendStringInfo(&message, ",%s", detail);
        appendStringInfoString(&message, "}\n");
        if (fwrite(message.data, 1, message.len, stream) != message.len || fflush(stream) != 0)
            elog(ERROR, "SIBench trace write failed");
        pfree(message.data);
    }
    /* An aborted transaction never reaches ExecutorEnd for its active executors. */
    if (executor_end && sibench_depth > 0) sibench_depth--;
    if (transaction_abort) sibench_depth = 0;
}

static void
sib_datum(StringInfo out, Oid type, Datum datum, bool isnull)
{
    Oid output;
    bool variable;
    char *text;
    if (isnull) { appendStringInfoString(out, "null"); return; }
    getTypeOutputInfo(type, &output, &variable);
    text = OidOutputFunctionCall(output, datum);
    escape_json(out, text);
    pfree(text);
}

/*
 * The elements of an array scan key (= ANY), so the scan's values are facts rather than an opaque
 * array. A row comparison has no single-column meaning and stays marked unsupported.
 */
static void
sib_scan_key(StringInfo out, ScanKey k, Oid type)
{
    ArrayType *array;
    Oid element;
    int16 length;
    bool byval;
    char align;
    Datum *values;
    bool *nulls;
    int count, i;
    if (k->sk_flags & SK_ROW_HEADER) {
        appendStringInfoString(out, "null,\"unsupported_key\":true");
        return;
    }
    if (!(k->sk_flags & SK_SEARCHARRAY)) {
        sib_datum(out, type, k->sk_argument, (k->sk_flags & SK_ISNULL) != 0);
        return;
    }
    appendStringInfoString(out, "null");
    if (k->sk_flags & SK_ISNULL) {
        appendStringInfoString(out, ",\"array\":null");
        return;
    }
    array = DatumGetArrayTypeP(k->sk_argument);
    element = ARR_ELEMTYPE(array);
    get_typlenbyvalalign(element, &length, &byval, &align);
    deconstruct_array(array, element, length, byval, align, &values, &nulls, &count);
    appendStringInfo(out, ",\"array_type_oid\":%u,\"array\":[", element);
    for (i = 0; i < count; i++) {
        if (i) appendStringInfoChar(out, ',');
        sib_datum(out, element, values[i], nulls[i]);
    }
    appendStringInfoChar(out, ']');
}

/* The fields every index scan event shares: scan identity, snapshot, TID and scan keys. */
static void
sib_scan_detail(StringInfo out, IndexScanDesc scan, ScanKey keys, int nkeys,
                ItemPointer tid, int direction, bool found)
{
    int i;
    appendStringInfo(out, "\"scan\":\"%p\",\"heap_oid\":%u,\"direction\":%d,\"found\":%s,\"recheck\":%s",
                     scan, scan->indexRelation->rd_index->indrelid, direction,
                     found ? "true" : "false", scan->xs_recheck ? "true" : "false");
    if (scan->xs_snapshot) {
        Snapshot s = scan->xs_snapshot;
        /* Only MVCC snapshots carry xmin/xmax/xip; others (e.g. planner endpoint probes use a
         * stack-allocated SnapshotNonVacuumable) leave those fields uninitialized.
         * csn is the transaction completion count at the time this snapshot was built: the counter
         * is incremented under ProcArrayLock exactly when a transaction stops counting as running,
         * so a commit is visible to a snapshot iff the snapshot's counter is at or after the
         * commit's. A copied snapshot loses the field, so it falls back to the counter of the last
         * snapshot this backend built, which is the same value under read committed, where every
         * statement builds a fresh snapshot before it runs. */
        if (s->snapshot_type == SNAPSHOT_MVCC) {
            appendStringInfo(out, ",\"snapshot_type\":%d,\"csn\":%llu,\"xmin\":%u,\"xmax\":%u,\"snapshot_cid\":%u,\"xip\":[",
                             s->snapshot_type,
                             (unsigned long long) (s->snapXactCompletionCount != 0
                                     ? s->snapXactCompletionCount
                                     : sibench_snapshot_csn),
                             s->xmin, s->xmax, s->curcid);
            for (i = 0; i < s->xcnt; i++) appendStringInfo(out, "%s%u", i ? "," : "", s->xip[i]);
            appendStringInfo(out, "],\"suboverflowed\":%s", s->suboverflowed ? "true" : "false");
        } else {
            appendStringInfo(out, ",\"snapshot_type\":%d", s->snapshot_type);
        }
    }
    if (tid && ItemPointerIsValid(tid))
        appendStringInfo(out, ",\"tid\":\"(%u,%u)\"", ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
    appendStringInfoString(out, ",\"keys\":[");
    for (i = 0; i < nkeys; i++) {
        ScanKey k = keys + i;
        Oid type = k->sk_subtype;
        if (!OidIsValid(type)) type = TupleDescAttr(RelationGetDescr(scan->indexRelation), k->sk_attno - 1)->atttypid;
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
    if (!sib_relation(scan->indexRelation)) return;
    initStringInfo(&out);
    sib_scan_detail(&out, scan, keys, nkeys, tid, direction, found);
    sib_emit(event, scan->indexRelation, out.data);
    pfree(out.data);
}

/* Appends the text of the given heap columns of a fetched row as a JSON array. */
static void
sib_row_columns(StringInfo out, TupleTableSlot *slot, const AttrNumber *columns, int count)
{
    int i;
    bool isnull;
    Datum value;
    appendStringInfoChar(out, '[');
    for (i = 0; i < count; i++) {
        if (i) appendStringInfoChar(out, ',');
        if (columns[i] <= 0) {
            appendStringInfoString(out, "null");
            continue;
        }
        value = slot_getattr(slot, columns[i], &isnull);
        sib_datum(out, TupleDescAttr(slot->tts_tupleDescriptor, columns[i] - 1)->atttypid,
                  value, isnull);
    }
    appendStringInfoChar(out, ']');
}

/*
 * The heap verdict of one index entry. A visible row also carries its identity, the values of the
 * scanning index's key columns and of the table's primary key, read from the visible heap tuple:
 * a scan its consumer stopped early returned exactly these rows, and their primary keys are the
 * only identity a KV index shares with PostgreSQL (heap TIDs have no counterpart).
 */
static void
sib_heap_visibility(IndexScanDesc scan, TupleTableSlot *slot, bool found)
{
    StringInfoData out;
    Bitmapset *primary;
    AttrNumber columns[INDEX_MAX_KEYS];
    int count = 0, member = -1;
    if (!sib_relation(scan->indexRelation)) return;
    initStringInfo(&out);
    sib_scan_detail(&out, scan, NULL, 0, found ? &slot->tts_tid : &scan->xs_heaptid, 0, found);
    if (found) {
        appendStringInfoString(&out, ",\"row\":{\"indexed\":");
        sib_row_columns(&out, slot, scan->indexRelation->rd_index->indkey.values,
                        scan->indexRelation->rd_index->indnkeyatts);
        primary = RelationGetIndexAttrBitmap(scan->heapRelation, INDEX_ATTR_BITMAP_PRIMARY_KEY);
        while ((member = bms_next_member(primary, member)) >= 0 && count < INDEX_MAX_KEYS)
            columns[count++] = member + FirstLowInvalidHeapAttributeNumber;
        bms_free(primary);
        appendStringInfoString(&out, ",\"pk\":");
        sib_row_columns(&out, slot, columns, count);
        appendStringInfoChar(&out, '}');
    }
    sib_emit("heap_visibility", scan->indexRelation, out.data);
    pfree(out.data);
}

/* Heap visibility of one bitmap candidate TID, for every index scan that contributed it. */
static void
sib_bitmap_verdict(Relation heap, void *node, ItemPointer tid, bool found)
{
    char detail[160];
    if (!sib_relation(heap)) return;
    snprintf(detail, sizeof(detail), "\"heap\":\"%p\",\"tid\":\"(%u,%u)\",\"found\":%s", node,
             ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid), found ? "true" : "false");
    sib_emit("bitmap_heap_candidate", heap, detail);
}

/* Binds a live index scan descriptor to its plan node; emitted when the descriptor is created. */
static void
sib_plan_scan(const char *kind, int plan_node_id, IndexScanDesc scan)
{
    char detail[160];
    if (!scan || !sib_relation(scan->indexRelation)) return;
    snprintf(detail, sizeof(detail), "\"scan\":\"%p\",\"plan_node\":%d,\"kind\":\"%s\"",
             (void *) scan, plan_node_id, kind);
    sib_emit("plan_scan", scan->indexRelation, detail);
}

static void
sib_index_insert(Relation relation, Datum *values, bool *isnull, ItemPointer tid, int unique)
{
    StringInfoData out;
    int i;
    if (!sib_relation(relation)) return;
    initStringInfo(&out);
    appendStringInfo(&out, "\"heap_oid\":%u,\"tid\":\"(%u,%u)\",\"unique_check\":%d,\"values\":[",
                     relation->rd_index->indrelid, ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid), unique);
    for (i = 0; i < RelationGetDescr(relation)->natts; i++) {
        if (i) appendStringInfoChar(&out, ',');
        sib_datum(&out, TupleDescAttr(RelationGetDescr(relation), i)->atttypid, values[i], isnull[i]);
    }
    appendStringInfoChar(&out, ']');
    sib_emit("index_insert_attempt", relation, out.data);
    pfree(out.data);
}
#endif
