#include "postgres.h"
#include "fmgr.h"
#include "commands/trigger.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "utils/relcache.h"
#include "sibench_capture.h"
#include "utils/json.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(sibench_row_capture);
PG_FUNCTION_INFO_V1(sibench_identity_capture);

/* The engine allocates RowIDs transactionally; capture retains the allocation even after Abort. */
Datum sibench_identity_capture(PG_FUNCTION_ARGS)
{
    TriggerData *trigger;
    bool isnull;
    Datum row_id, key;
    StringInfoData output;
    if (!CALLED_AS_TRIGGER(fcinfo)) elog(ERROR,"sibench_identity_capture requires trigger context");
    trigger=(TriggerData *)fcinfo->context;
    if (!TRIGGER_FIRED_BY_INSERT(trigger->tg_event) || trigger->tg_trigger->tgnargs!=1)
        elog(ERROR,"sibench_identity_capture requires an insert and a table ID");
    row_id=heap_getattr(trigger->tg_trigtuple,1,RelationGetDescr(trigger->tg_relation),&isnull);
    if (isnull || DatumGetInt64(row_id)<=0) elog(ERROR,"Invalid native RowID allocation");
    initStringInfo(&output);
    appendStringInfo(&output,"\"table_id\":%d,\"row_id\":%lld,\"primary_key\":",
        pg_strtoint32(trigger->tg_trigger->tgargs[0]),(long long)DatumGetInt64(row_id));
    key=heap_getattr(trigger->tg_trigtuple,2,RelationGetDescr(trigger->tg_relation),&isnull);
    if(isnull)elog(ERROR,"Null primary key in native identity allocation");
    sib_datum(&output,TupleDescAttr(RelationGetDescr(trigger->tg_relation),1)->atttypid,key,false);
    sib_emit("row_identity",trigger->tg_relation,output.data);
    pfree(output.data);
    PG_RETURN_POINTER(trigger->tg_trigtuple);
}

static void append_tuple(StringInfo output, Relation relation, HeapTuple tuple, Bitmapset *indexed)
{
    int i;
    bool first = true;
    if (!tuple) { appendStringInfoString(output, "null"); return; }
    appendStringInfoChar(output, '{');
    for (i=0; i<RelationGetDescr(relation)->natts; i++) {
        Form_pg_attribute attribute = TupleDescAttr(RelationGetDescr(relation), i);
        bool isnull;
        Datum value;
        if (!bms_is_member(i+1-FirstLowInvalidHeapAttributeNumber,indexed)) continue;
        value = heap_getattr(tuple, i+1, RelationGetDescr(relation), &isnull);
        if (!first) appendStringInfoChar(output, ',');
        first = false;
        escape_json(output, NameStr(attribute->attname));
        appendStringInfoChar(output, ':');
        sib_datum(output, attribute->atttypid, value, isnull);
    }
    appendStringInfoChar(output, '}');
}

Datum sibench_row_capture(PG_FUNCTION_ARGS)
{
    TriggerData *trigger;
    HeapTuple old = NULL, next = NULL;
    const char *operation;
    StringInfoData output;
    if (!CALLED_AS_TRIGGER(fcinfo)) elog(ERROR, "sibench_row_capture requires trigger context");
    trigger = (TriggerData *) fcinfo->context;
    if (TRIGGER_FIRED_BY_UPDATE(trigger->tg_event)) {
        old = trigger->tg_trigtuple; next = trigger->tg_newtuple; operation="update";
    } else if (TRIGGER_FIRED_BY_DELETE(trigger->tg_event)) {
        old = trigger->tg_trigtuple; operation="delete";
    } else { next = trigger->tg_trigtuple; operation="insert"; }
    if (sib_enabled()) {
        /* Native history owns row contents; projection needs only indexed attributes. */
        Bitmapset *indexed = RelationGetIndexAttrBitmap(trigger->tg_relation,INDEX_ATTR_BITMAP_ALL);
        initStringInfo(&output);
        appendStringInfo(&output,"\"operation\":\"%s\",\"old\":",operation);
        append_tuple(&output, trigger->tg_relation, old, indexed);
        appendStringInfoString(&output,",\"new\":");
        append_tuple(&output, trigger->tg_relation, next, indexed);
        if (old) appendStringInfo(&output,",\"old_tid\":\"(%u,%u)\"",ItemPointerGetBlockNumber(&old->t_self),ItemPointerGetOffsetNumber(&old->t_self));
        sib_emit("row_change",trigger->tg_relation,output.data);
        pfree(output.data);
        bms_free(indexed);
    }
    PG_RETURN_POINTER(next ? next : old);
}

PG_FUNCTION_INFO_V1(sibench_query_id);
Datum sibench_query_id(PG_FUNCTION_ARGS)
{
    if (!sib_enabled() || !sibench_request)
        elog(ERROR, "SIBench history writes require a captured source session");
    PG_RETURN_INT64((int64) sibench_request);
}

PG_FUNCTION_INFO_V1(sibench_capture_watermark);
Datum sibench_capture_watermark(PG_FUNCTION_ARGS)
{
    int pid = PG_GETARG_INT32(0);
    int64 sequence = PG_GETARG_INT64(1), csn = PG_GETARG_INT64(2);
    int64 consumed = PG_GETARG_INT64(3);
    if (pid < 0 || sequence < 0 || csn < 0 || consumed < 0 || consumed > sequence)
        elog(ERROR, "Invalid source acknowledgement");
    PG_RETURN_INT64((int64) sib_capture_watermark(pid, (uint64) sequence, (uint64) csn, (uint64) consumed));
}
