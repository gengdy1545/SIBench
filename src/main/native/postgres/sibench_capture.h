/* PostgreSQL 14.24 capture instrumentation. Included once per backend translation unit. */
#ifndef SIBENCH_CAPTURE_H
#define SIBENCH_CAPTURE_H
#include "access/genam.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include <stdio.h>
#include <unistd.h>

extern char *application_name;
extern IndexScanDesc sibench_bitmap_scan;
/*
 * Per-backend request identity, shared by every translation unit that includes this header.
 * sibench_request counts top-level executions (the first executor_start outside any executor);
 * sibench_depth is the number of executors currently active, so SPI/trigger queries are nested
 * under their parent request and planner-time probes are visible as depth 0.
 */
extern int sibench_depth;
extern uint64 sibench_request;

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

static void
sib_scan(const char *event, IndexScanDesc scan, ScanKey keys, int nkeys,
         ItemPointer tid, int direction, bool found)
{
    StringInfoData out;
    int i;
    if (!sib_relation(scan->indexRelation)) return;
    initStringInfo(&out);
    appendStringInfo(&out, "\"scan\":\"%p\",\"heap_oid\":%u,\"direction\":%d,\"found\":%s,\"recheck\":%s",
                     scan, scan->indexRelation->rd_index->indrelid, direction,
                     found ? "true" : "false", scan->xs_recheck ? "true" : "false");
    if (scan->xs_snapshot) {
        Snapshot s = scan->xs_snapshot;
        /* Only MVCC snapshots carry xmin/xmax/xip; others (e.g. planner endpoint probes use a
         * stack-allocated SnapshotNonVacuumable) leave those fields uninitialized. */
        if (s->snapshot_type == SNAPSHOT_MVCC) {
            appendStringInfo(&out, ",\"snapshot_type\":%d,\"xmin\":%u,\"xmax\":%u,\"snapshot_cid\":%u,\"xip\":[",
                             s->snapshot_type, s->xmin, s->xmax, s->curcid);
            for (i = 0; i < s->xcnt; i++) appendStringInfo(&out, "%s%u", i ? "," : "", s->xip[i]);
            appendStringInfoChar(&out, ']');
        } else {
            appendStringInfo(&out, ",\"snapshot_type\":%d", s->snapshot_type);
        }
    }
    if (tid && ItemPointerIsValid(tid))
        appendStringInfo(&out, ",\"tid\":\"(%u,%u)\"", ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
    appendStringInfoString(&out, ",\"keys\":[");
    for (i = 0; i < nkeys; i++) {
        ScanKey k = keys + i;
        Oid type = k->sk_subtype;
        if (!OidIsValid(type)) type = TupleDescAttr(RelationGetDescr(scan->indexRelation), k->sk_attno - 1)->atttypid;
        appendStringInfo(&out, "%s{\"attribute\":%d,\"strategy\":%d,\"flags\":%d,\"type_oid\":%u,\"value\":",
                         i ? "," : "", k->sk_attno, k->sk_strategy, k->sk_flags, type);
        if (k->sk_flags & (SK_ROW_HEADER | SK_SEARCHARRAY)) appendStringInfoString(&out, "null,\"unsupported_key\":true");
        else sib_datum(&out, type, k->sk_argument, (k->sk_flags & SK_ISNULL) != 0);
        appendStringInfoChar(&out, '}');
    }
    appendStringInfoChar(&out, ']');
    sib_emit(event, scan->indexRelation, out.data);
    pfree(out.data);
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
