/*
 * PostgreSQL 14.24 capture instrumentation: declarations shared by the patched backend files and
 * the row trigger module. The implementation is sibench_capture.c, linked into the backend.
 *
 * By default an index access is recorded as one summary when it ends, not one event per row:
 * the counters, last-run identities and bitmap verdict attributions the access ledger needs, all
 * accumulated from the same executor hooks. SIBENCH_CAPTURE_DETAIL=rows additionally keeps every
 * per-row event, so one native run carries both and the ledger checks that they agree.
 */
#ifndef SIBENCH_CAPTURE_H
#define SIBENCH_CAPTURE_H
#include "access/genam.h"
#include "access/relscan.h"
#include "executor/execdesc.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "tcop/dest.h"
#include "utils/rel.h"

extern char *application_name;
/* The index scan filling a bitmap, while its access method fills it. */
extern IndexScanDesc sibench_bitmap_scan;
/* The bitmap heap scan node whose page heapam is checking, while it checks it. */
extern void *sibench_bitmap_heap_node;
/*
 * Per-backend request identity. sibench_request counts top-level executions (the first
 * executor_start outside any executor); sibench_depth is the number of executors currently active,
 * so SPI/trigger queries are nested under their parent request and planner-time probes are
 * visible as depth 0.
 */
extern int sibench_depth;
extern uint64 sibench_request;

extern bool sib_enabled(void);
extern bool sib_relation(Relation relation);
extern void sib_emit(const char *event, Relation relation, const char *detail);
extern void sib_datum(StringInfo out, Oid type, Datum datum, bool isnull);

/* Executor and transaction boundaries. */
extern void sib_executor_start(QueryDesc *queryDesc);
extern DestReceiver *sib_wrap_receiver(DestReceiver *original);
extern void sib_executor_run_end(DestReceiver *dest, uint64 processed);
extern void sib_executor_end(uint64 processed);
extern void sib_transaction_commit(void);
extern void sib_transaction_abort(void);

/* Index scans. */
extern void sib_plan_scan(const char *kind, int plan_node_id, IndexScanDesc scan);
extern void sib_index_rescan(IndexScanDesc scan, ScanKey keys, int nkeys);
extern void sib_index_end(IndexScanDesc scan);
extern void sib_index_next(IndexScanDesc scan, ScanDirection direction, bool found);
extern void sib_heap_visibility(IndexScanDesc scan, TupleTableSlot *slot, bool found);
extern void sib_vm_visible(IndexScanDesc scan, ItemPointer tid);

/* Bitmap scans. */
extern void sib_bitmap_candidates(IndexScanDesc scan, const ItemPointer tids, int ntids);
extern void sib_bitmap_end(IndexScanDesc scan);
extern void sib_bitmap_binding(BitmapHeapScanState *node);
extern void sib_bitmap_skipped(BitmapHeapScanState *node, TBMIterateResult *tbmres);
extern void sib_bitmap_verdict(Relation heap, void *node, ItemPointer tid, bool found);
extern void sib_bitmap_lossy(Relation heap, void *node);

/* Writes. */
extern void sib_index_insert(Relation relation, Datum *values, bool *isnull, ItemPointer tid, int unique);
extern void sib_index_build_start(Relation index);
extern void sib_index_build_end(Relation index, double heap_rows, double index_rows);
#endif
