#include "postgres.h"
#include "fmgr.h"
#include "commands/trigger.h"
#include "access/htup_details.h"
#include "sibench_capture.h"
#include "utils/json.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(sibench_row_capture);

static void append_tuple(StringInfo output, Relation relation, HeapTuple tuple)
{
    int i;
    if (!tuple) { appendStringInfoString(output, "null"); return; }
    appendStringInfoChar(output, '{');
    for (i=0; i<RelationGetDescr(relation)->natts; i++) {
        Form_pg_attribute attribute = TupleDescAttr(RelationGetDescr(relation), i);
        bool isnull;
        Datum value = heap_getattr(tuple, i+1, RelationGetDescr(relation), &isnull);
        if (i) appendStringInfoChar(output, ',');
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
        initStringInfo(&output);
        appendStringInfo(&output,"\"operation\":\"%s\",\"old\":",operation);
        append_tuple(&output, trigger->tg_relation, old);
        appendStringInfoString(&output,",\"new\":");
        append_tuple(&output, trigger->tg_relation, next);
        if (old) appendStringInfo(&output,",\"old_tid\":\"(%u,%u)\"",ItemPointerGetBlockNumber(&old->t_self),ItemPointerGetOffsetNumber(&old->t_self));
        if (next) appendStringInfo(&output,",\"new_tid\":\"(%u,%u)\"",ItemPointerGetBlockNumber(&next->t_self),ItemPointerGetOffsetNumber(&next->t_self));
        sib_emit("row_change",trigger->tg_relation,output.data);
        pfree(output.data);
    }
    PG_RETURN_POINTER(next ? next : old);
}
