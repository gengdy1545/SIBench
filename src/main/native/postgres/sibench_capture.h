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
    if (!sib_enabled()) return;
    directory = getenv("SIBENCH_CAPTURE_DIR");
    if (!directory || !directory[0]) return;
    if (!stream) {
        snprintf(path, sizeof(path), "%s/pg-%d.jsonl", directory, MyProcPid);
        stream = fopen(path, "a");
        if (!stream) elog(ERROR, "SIBench cannot open trace file %s", path);
        setvbuf(stream, NULL, _IOLBF, 65536);
    }
    initStringInfo(&message);
    appendStringInfo(&message, "{\"event\":\"%s\",\"pid\":%d,\"xid\":%u,\"cid\":%u,\"application\":",
                     event, MyProcPid, GetTopTransactionIdIfAny(), GetCurrentCommandId(false));
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
        appendStringInfo(&out, ",\"snapshot_type\":%d,\"xmin\":%u,\"xmax\":%u,\"snapshot_cid\":%u,\"xip\":[",
                         s->snapshot_type, s->xmin, s->xmax, s->curcid);
        for (i = 0; i < s->xcnt; i++) appendStringInfo(&out, "%s%u", i ? "," : "", s->xip[i]);
        appendStringInfoChar(&out, ']');
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
