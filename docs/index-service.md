# Index Service contract

The protocol namespace is `sibench.index`, defined in `src/main/proto/index_service.proto`. The driver and service are built together from the same commit. The service executes operations by transaction position and publishes complete commits as a contiguous sequence, using canonical Trace inputs.

## Interfaces and acknowledgment boundaries

| Interface | Semantics |
| --- | --- |
| `Describe` | Declares value types, versioned reads, overwrites at the same version, transaction-local reads, query groups, complete-commit publication, Baseline batch writes, and acknowledgment / Flush durability levels. The driver rejects services that lack complete-commit publication or capabilities required by the input. |
| `Execute` | One bidirectional stream per client. Initial metadata `sibench-index-binding` must carry the binding for this Trace, value mode, and secondary-index configuration. The service checks it and binds the index before accepting requests. Request IDs are unique within a stream; responses match by ID and may complete out of order. Operations within a transaction execute according to `transaction_id` / `transaction_operation`; different transactions may run in parallel. Connection arrival order cannot determine version visibility. |
| `Flush` | Waits for accepted requests to reach the declared durability level, records the complete publication watermark and Trace binding, and returns the index location. It defines load, workload, and recovery boundaries; the driver does not use it as a per-request barrier to repair query ordering. |

`Describe` uses `replay.request_timeout_seconds`. Both the client deadline for `Flush` and the server's request-drain deadline are 3,600 seconds. Any failed accepted request makes Flush fail. The driver also checks the returned binding, exact publication watermark, and nonempty index location.

Writes with unknown outcomes are not retried automatically. `INVALID` means the request violates the contract; `UNAVAILABLE` means execution failed and a write may already have taken effect. The first error, subsequent drain, and process cleanup are recorded separately.

A local send acknowledgment confirms serialization and handoff to the transport;
it does not establish remote receipt or execution. Merely enqueueing a message
does not satisfy this boundary. Checked responses complete separately. Buffered
batch/group members are counted separately from actual RPCs.

## Data, transactions, and reads

An index entry is `(table, index, indexed_value, PK)`; the primary index, `index=0`, has no indexed value. Key components use `IndexKey`'s order-preserving encoding. Values are either RowIDs or Locators `(file, position)` containing two full int64 values. Capabilities are checked separately for each mode.

| Operation | Constraints |
| --- | --- |
| Baseline `INSERT` | Version 0; bypasses WAL, with declared durability guaranteed only after Flush. `WriteBatch` is allowed. The entire batch is validated first, and an invalid member fails the batch. When a batch shares completion state, in-flight budgets, completion counts, and latency remain per member, using the earliest deadline in the batch. |
| `UPSERT` / `LOGICAL_DELETE` | Writes at a new version. Operations on the same key and version overwrite according to transaction position. Deletion retains an invisible version and must not resurrect older values. |
| `RELOCATE` | Must match an existing exact version. Changes its physical location without creating a logical version. This does not imply that the full Rewrite / GC protocol is implemented. |
| `Search` | At read_version, selects the latest version at or below the read point, omitting entries whose selected version is deleted. Supports primary / secondary ranges, inclusive and exclusive bounds, direction, limit, and secondary lookups pinned to a PK. Results are ordered by indexed value and PK, reversed for descending queries. |
| Transaction-local `Search` | read_version preserves the original statement snapshot; local_version identifies this transaction's exact version. Only writes from earlier transaction_operation positions in the same transaction are overlaid. Other intervening commits remain invisible. |
| `SearchGroup` | The server intersects or unions members of the same source access by PK. Intersection follows the first member's order; union preserves first-occurrence order. Members must share the table, read point, and group identity. |
| `Commit` | Declares the transaction version and actual sent write and operation counts. Operations include queries, groups, and writes. Transactions made empty by filtering still have commit markers. |

Publishing version V requires its commit marker, completion of all its operations, matching write counts, and publication of V−1. The maximum observed version cannot replace the contiguous complete watermark. Snapshot reads wait for publication of read_version; transaction-local reads also wait for earlier positions in the same transaction.

Late writes to published versions, version reuse across transactions, and duplicate
pending/executing positions are rejected before reaching the backend. A rejected
request must not change state. Transaction-identity bookkeeping grows with the
maximum transaction_id; prefix reclamation is not implemented.

## Results and validation

Ordinary queries return chunks of 16,384 results. `more=true` indicates another chunk with the same ID; only the last chunk carries the final status. The service respects gRPC flow control, and the driver counts results chunk by chunk without retaining the entire ordinary query result. Query groups are fully materialized, so their memory requirements grow with member results and require capacity validation for the intended workload.

Each query checks the native expected count, response status and chunk completion. Missing or extra result items, invalid partial responses and incomplete request consumption fail the replay. Equal counts do not establish matching keys, versions, values or order; replay does not claim that guarantee.

Replay consumes canonical requests and native counts, without per-query answer artifacts or a reference index. Input integrity, source bindings, capability checks, transaction ordering and exact Flush publication remain required.

`request_bytes` sums `Request.getSerializedSize()`, including batch requests. `result_bytes` sums `Result.getSerializedSize()`, accounted after the final response arrives and its count is verified. These counters exclude response envelopes, repeated-field tags / lengths, empty responses, gRPC / HTTP2 / TCP overhead, and retransmissions; they do not measure network bytes.

## Publication and reopening

`Flush` waits for requests accepted before the call began, then writes the publication watermark and Trace binding into the index and persists them with the data. An index rejects a binding for another Trace, value mode, or secondary-index configuration. A minimal index manifest beside the report records input identity, version coverage, key / value encoding, backend location, build identity, and replay configuration. Source snapshot is N/A until integrated.

`reopen: true` opens an existing index and restores the last Flush watermark; versions above it remain invisible. `verify-service` checks all replayable snapshot queries at their original read points. Transaction-local queries are reported separately and excluded from this check because they depend on live transaction prefixes that cannot be reconstructed by replay after the transaction has ended.

## Local RocksDB

Local RocksDB uses the [Trace key encoding](trace.md#identity-and-versions).
Baseline bypasses WAL; other writes use local WAL without fsync. Acknowledgment
therefore does not promise power-loss durability.

See [README](../README.md#replay-and-service) for service configuration and replay
entry points. Both sides must agree on value mode and secondary-index filtering.

## rocksdb-cloud backend

The thin JNI layer forwards bytes and reuses the same Java key layout, version selection, and publication logic. See [README](../README.md#rocksdb-cloud-jni) for build and loading instructions.

| Item | Semantics |
| --- | --- |
| Write acknowledgment | memtable + local WAL, without fsync; Baseline bypasses WAL. |
| Flush | Returns after SST / MANIFEST uploads are acknowledged, guaranteeing remote durability through the last Flush. Later writes may exist only locally. |
| Cold start | `keep_local_sst_files=true`. After loss of the local directory, required SSTs are downloaded from the bucket. WAL that was not uploaded cannot be recovered. |
| Deletion | No automatic bucket creation. `run_purger=false`; ordinary expired / invisible file deletion is delayed by 1 h by default. Bucket bytes may temporarily exceed local bytes. This is not logical version GC. |
| Measurement | Each Flush writes `<index>.engine-stats.jsonl`. Object requests are N/A for the local backend. |

Object requests record counts, failures, bytes, cumulative microseconds, and a fixed 32-bucket histogram by request type. Upper bounds are 1, 2, 4…2^30 μs, with the final bucket handling overflow: `overflow_after=2^30`, with overflow upper bound `Long.MAX_VALUE`. p50 / p95 / p99 are the upper bounds of the buckets containing their nearest ranks. Both successes and failures contribute; percentiles are not interpreted when samples=0. Concurrent snapshots are not transactional, and percentiles use each histogram's own sample count. Callbacks do not separately report SDK-internal HTTP retries. Cumulative parallel time cannot be added to wall time.

Java and JNI statistics formats must match; builds missing required histograms
are rejected at open. Engine-log analysis requires independently established
same-run provenance before supporting performance claims. Missing engine data is
reported as N/A.

## Validation boundaries

Required acceptance is one complete local replay and one real-cloud replay of the same Trace, including loading, response/native-count checks and Flush. See [README](../README.md#tests) for entry points. Normal reopening and fault recovery (forced termination, Flush fault injection, local-directory loss, cold recovery and power loss) are separate diagnostics outside this scope.

Correctness and resource reports must bind the same run, inputs, configuration and build. Unit tests, short inputs, clean reopening and storage-interface checks do not establish full-input capacity, fault recovery or backend performance rankings. Query-group capacity needs workload-specific validation.

Data-file Rewrite, physical version GC, retries for writes with unknown outcomes and the complete source-snapshot lifecycle are not implemented.
