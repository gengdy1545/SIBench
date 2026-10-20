#ifndef SIBENCH_RECEIVER_H
#define SIBENCH_RECEIVER_H
#include "tcop/dest.h"
#include "executor/tuptable.h"

typedef struct SibReceiver {
    DestReceiver pub;
    DestReceiver *original;
} SibReceiver;

static bool sib_receive(TupleTableSlot *slot, DestReceiver *receiver)
{
    SibReceiver *self = (SibReceiver *) receiver;
    StringInfoData out;
    int i;
    initStringInfo(&out);
    slot_getallattrs(slot);
    appendStringInfoString(&out, "\"values\":[");
    for (i = 0; i < slot->tts_tupleDescriptor->natts; i++) {
        if (i) appendStringInfoChar(&out, ',');
        sib_datum(&out, TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid,
                  slot->tts_values[i], slot->tts_isnull[i]);
    }
    appendStringInfoChar(&out, ']');
    sib_emit("result_row", NULL, out.data);
    pfree(out.data);
    return self->original->receiveSlot(slot, self->original);
}

static void sib_start(DestReceiver *receiver, int operation, TupleDesc typeinfo)
{
    SibReceiver *self = (SibReceiver *) receiver;
    self->original->rStartup(self->original, operation, typeinfo);
}

static void sib_shutdown(DestReceiver *receiver)
{
    SibReceiver *self = (SibReceiver *) receiver;
    self->original->rShutdown(self->original);
}

static void sib_destroy(DestReceiver *receiver)
{
    SibReceiver *self = (SibReceiver *) receiver;
    self->original->rDestroy(self->original);
    pfree(self);
}

static DestReceiver *sib_wrap_receiver(DestReceiver *original)
{
    SibReceiver *receiver = palloc0(sizeof(SibReceiver));
    receiver->original = original;
    receiver->pub.receiveSlot = sib_receive;
    receiver->pub.rStartup = sib_start;
    receiver->pub.rShutdown = sib_shutdown;
    receiver->pub.rDestroy = sib_destroy;
    receiver->pub.mydest = original->mydest;
    return (DestReceiver *) receiver;
}
#endif
