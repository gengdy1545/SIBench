# Index Service contract

[index_service.proto](../proto/index_service.proto) defines five RPCs in `sibench.index`. Keys and values are opaque bytes, including empty values and prefix-related keys. SIBench owns business-key encoding and RowID/Locator conversion. Client, service and persisted indexes use one coordinated representation; changes require matching clients and fresh indexes.

## Data interfaces

| RPC | Contract |
| --- | --- |
| Get | Exact key at `read_ts`; returns an optional value. Absent and present-empty are distinct. |
| Put | Key, caller-assigned `ts` and value. ACK follows same-instance visible application. |
| Delete | Key and `ts`. A selected tombstone hides older values without invalidating historical reads. |
| Scan | Unsigned byte range `[start_key,end_key)` at `read_ts`; omitted endpoints are unbounded. Ascending unless reversed; `limit=0` is unlimited. Returns one value per visible key in key order, preserving duplicates. |
| WriteBatch | Put/Delete members, validated before application; empty batches fail. ACK follows every member. Failure may be partial; atomic visibility and rollback are not promised. |

Scan is server-streaming; the other RPCs are unary. Reads return values only. Get/Scan select the newest version at or below `read_ts`. Timestamps and limits must fit nonnegative signed int64. Timestamp 0 uses the ordinary ACK policy. The service allocates no revisions and exposes no transaction, Describe or Flush RPC.

Reads may include ordered, unique-key `local_writes`, confined to their bounds and using timestamp zero. Private Puts replace snapshot values; Deletes hide them. Scan merges by key before direction/limit. Contexts are request-scoped, never persisted or shared, and limited to 1,000,000 entries and 32 MiB. This visibility is supplied by SIBench's shared Java layer over each backend; it does not certify native backend transactions. [Trace](trace.md#operations-and-index-expansion) defines source commits and private contexts.

ACK guarantees visibility to other clients of the same instance, not remote durability. Conflicting writes at one key/timestamp are unsupported. INVALID_ARGUMENT rejects invalid input. Cancellation, deadlines and UNAVAILABLE may leave write outcomes unknown; the driver does not retry them automatically. Local transport handoff proves neither receipt nor execution.

## Harness identity and deployment description

Replay attaches `sibench-index-binding` and `sibench-value-kind` metadata. The server binds a fresh index to its first Trace, value mode and secondary-index selection and refuses mismatches. Ordinary callers may omit these headers; they are not authentication.

The server returns JSON IndexDescription through binary `sibench-service-bin` metadata: backend, value mode, ACK level, WAL setting, durability and storage location. Replay requires a complete, consistent description on all responses, including empty Scans. Reports bind input/build/configuration and backend location. `last_source_version` describes consumed input, not a durability watermark.

## Baseline and incremental measurement

Replay batches baseline Puts using `baseline_batch_operations`; every incremental write or read member is a separate RPC. All dispatch settings in [config/sibench.yaml](../config/sibench.yaml) are required. `client_count × (batch − 1)` must be below `max_in_flight`. Baseline reports distinguish basic `operations`, `rpc_requests` and batch size; buffered members count as sent when batched, before transport handoff.

ReadGroup is a Trace description, not an RPC. The client combines member values as [defined by Trace](trace.md#operations-and-index-expansion) and checks the combined expected count. Group results can consume memory proportional to member results. A whole group must fit `max_in_flight` before admission.

Snapshot queries wait for pending write ACKs at/below their read point. The sender can defer them while sending newer writes; deferred queries leave in source order, and later writes at/below a deferred read point wait. Dependency/deferral waits and admission backpressure are outside API latency but inside elapsed throughput time.

Latency runs from client API entry through that operation's final response, including serialization/buffering. Scan ends at normal stream completion. Each group member keeps its own completion time; combined validation gates drain. Baseline and workload are separate phases. Warmup membership follows input order; warmup and failed requests are excluded from successful latency samples. Source markers have no call or latency.

Mean/max retain nanoseconds. p50/p95/p99 are nearest-rank upper bounds, exact below 2048 ns and with less than 1/1024 relative quantization error thereafter. Client completion is not an exact server-visibility timestamp. Background persistence and maintenance contribute to experiment resource costs.

## Results and failure handling

Scans respect flow control and chunk at 16,384 values or approximately 1 MiB. Larger individual values may travel alone up to the 32 MiB message bound. Oversized payloads fail. Partial results followed by RPC failure never count as success.

Replay checks input/source integrity, full consumption, binding/value mode, status/completion and native expected counts. **Matching counts do not certify values or order.** Ordinary replay creates no reference index or per-query answer artifact. Independent acceptance must compare distinguishable historical values and deletion/reinsertion cases; the value-only API cannot expose selected keys/timestamps.

| Report field | Unit |
| --- | --- |
| `target_calls` | Basic operations, including individual baseline batch members. |
| `rpc_requests`, `rpc_by_kind` | Actual RPCs by method. |
| `request_bytes` | Serialized request payloads, including batch envelopes/private contexts. |
| `result_bytes` | Value bytes received, including group members before deduplication. |
| `checked_result_items` | Validated ordinary or combined-group results. |
| `private_context_reads`, `private_context_entries` | Dispatched read members with contexts and their entries. |

Byte counts exclude transport overhead; result bytes also exclude response envelopes. They are not network-byte measurements.

## Internal persistence and reopening

The service checks dirty state every five seconds and persists completed writes, including after input becomes idle. This is not a per-write deadline. Partial write failures can leave dirty state. Persistence failure rejects later calls and fails controlled shutdown; cleanup preserves the original request error.

Controlled shutdown bounds transport/execution waits to 30 seconds each, stops maintenance and persists dirty writes. Native handles remain valid until workers/persistence exit; a timeout does not authorize closing active handles. Successful replay establishes request completion and visibility, not durability of every ACK.

Set `service.reopen: true` to open persisted state and validate binding/value kind. `verify-service CONFIG BENCHMARK REPORT` checks original snapshot queries and rejects query-free Traces; it does not force persistence. Reopen after a completed persistence cycle or controlled shutdown. Clean reopen does not establish crash recovery, cross-instance visibility or autoscaling.

## Backends

[Build and service commands](../README.md#build) use pinned sources and fresh outputs. The comparison targets are RocksDB-Cloud and SlateDB; local RocksDB is a generation/test fixture. No physical version GC or unknown-outcome retry is supported.

| Backend | Write ACK | Persistence and storage |
| --- | --- | --- |
| RocksDB-Cloud | `service.wal: false`: memory (`ACK_MEMORY`); true: local WAL without fsync (`ACK_WAL`). | Persistence flushes/uploads SSTs and MANIFEST. Later writes can be lost before persistence; unuploaded WAL is unrecoverable. Local SST copies are retained; missing copies download on reopen. |
| SlateDB | Memory-visible (`ACK_MEMORY`) under either WAL setting. | With WAL, persistence waits for uploaded WAL; without WAL, it writes an L0 SST. Clean close flushes either mode. SSTs have local mirrors; WAL/manifests stay remote. |
| Local RocksDB | Local WAL without fsync. | No power-loss durability promise; retained historical tombstones. |

`index-server CONFIG SERVICE_CONFIG` resolves the backend's `service.storage` URI through CONFIG's shared object-storage connections; see [selection and storage](trace.md#selection-and-storage). `service.wal` is required and applies equally to baseline and workload writes; both cloud templates set false. Buckets must exist. SlateDB refuses existing prefixes on creation and depends on conditional object writes for ownership, fencing and publication. Verify these primitives on the actual deployment. On endpoints that ignore PUT conditions, the adapter serializes/checks conditional PUTs within one process only: **run exactly one SlateDB service process per object prefix**. This cannot fence other processes or certify deployment durability.

Cloud backends report engine and remote-request statistics below local copies in `<index>.engine-stats.jsonl`. Missing data is N/A. Object latency percentiles are nearest-rank bucket upper bounds over successes/failures; cumulative parallel latency is not wall time, and SDK-internal retries are not separately counted. Build success and reopening do not certify full-input capacity or recovery.
