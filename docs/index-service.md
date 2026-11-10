# Index Service contract

[index_service.proto](../proto/index_service.proto) defines the five data RPCs in namespace `sibench.index`. Shared logical types are in [index_types.proto](../proto/index_types.proto); stored operations and source facts belong to [index_trace.proto](../proto/index_trace.proto). Driver and service ship together with one supported API and Trace format.

## Data interfaces

| RPC | Contract |
| --- | --- |
| `Get` | Exact `(table,index,indexed_value,PK)` at `read_version`; one optional Result. Missing Result means absent; RowID/Locator zero is valid. A secondary lookup without an exact PK uses Scan. |
| `Put` | One entry and logical version; success follows visible application through the same service instance. Version 0 uses the same write/ACK policy as every other version. |
| `Delete` | Versioned logical deletion; the selected tombstone hides older values while historical reads remain valid. |
| `Scan` | Ordered bounded query at `read_version`, with inclusive/exclusive endpoints, direction and limit. Chunks concatenate in order; only normal gRPC completion establishes success, including for an empty result. |
| `BatchWrite` | Generic Put/Delete members; success follows completion of every member. Inputs are validated before application. Atomic visibility and rollback are not promised; execution failure may be partial. |

Get/Put/Delete/BatchWrite are unary calls; Scan is server streaming. Requests reuse per-client channels and run concurrently. gRPC supplies call association, errors and stream completion; the data API carries no request IDs, transaction control, Describe or Flush RPCs.

The service consumes committed updates and committed-snapshot queries. Generation excludes aborted writes and transaction-local searches and retains the final write to each physical identity within one commit version. Source transaction boundaries remain in Trace for provenance and input validation; the service does not infer missing commits or wait for source transactions.

An entry is `(table,index,indexed_value,PK)`; primary index 0 has no indexed value. Components use the [Trace encoding](trace.md#identity-and-versions). Values are RowID or Locator `(file,position)`, retaining both full int64 fields. Get/Scan select the latest version at or below their explicit read point. Versions and limits must fit Java's nonnegative signed int64 range; read version 0 is valid. Repeated conflicting writes to the same physical identity/version and unknown-outcome retries are outside this contract. Current production sources do not emit relocation/GC workloads; RELOCATE is not a public data RPC.

A successful write ACK guarantees same-instance visibility, including another client's RPCs. It does not promise remote durability. INVALID_ARGUMENT rejects invalid input; UNAVAILABLE, cancellation and deadlines may leave a write outcome unknown. The driver never automatically retries such writes. Local transport handoff is measured separately and does not establish server receipt, execution or success.

## Harness identity and deployment description

SIBench attaches `sibench-index-binding` and `sibench-value-kind` metadata to its data calls. The server adapter checks both before applying the request and binds a fresh index to its first Trace; another Trace, value mode or secondary-index selection is refused. This is a harness guard around the data API. Ordinary callers may use the five RPCs without SIBench metadata; it is not an authentication mechanism.

The adapter returns a JSON IndexDescription in binary `sibench-service-bin` response metadata: backend name, value mode, acknowledgment level, durability description and storage location. Replay checks the selected mode, complete description and consistency across calls before accepting responses. These are deployment/reporting facts rather than data messages. All five RPCs and versioned reads are required by this API; query groups are implemented by SIBench, so no optional service group capability remains.

A replay report records the Trace/input/build/configuration identity and backend location. `last_source_version` comes from consumed Trace, not a durability or publication watermark. No additional lookup, polling read or independent DbReader is added to ordinary replay.

## Baseline and incremental measurement

Baseline load uses BatchWrite, bounded by the in-flight budget. Only baseline operations are combined by replay; the public batch method itself is generic. Every incremental write or query member is a separate data RPC. Loading and workload throughput/latency are reported in separate phases. A source update can expand into multiple independent index writes, including Delete(old secondary key) and Put(new secondary key).

Every basic operation is timed from entry to the Index Client API through completion of its own final business response, using the replay process's monotonic clock. Serialization and client buffering are included. Scan ends only after normal stream completion. Admission waits at the in-flight limit are separate backpressure. Background persistence and engine maintenance run during the experiment and contribute to its resource costs.

`target_calls` counts basic operations; `rpc_requests` counts actual calls, with `rpc_by_kind` distinguishing Get/Put/Delete/Scan/BatchWrite. Baseline batch members remain distinct basic operations. Trace source boundaries are not sent and do not enter throughput or latency. Warmup membership is assigned in input order before query deferral and follows each operation through its response; histograms exclude warmup. Failed or timed-out requests fail the run and are not successful latency samples.

SearchGroup remains a source-access description in Trace. The client issues one Get/Scan per member, unions/intersects results by PK, and checks the combined native expected count. Intersection preserves the first member's order; union preserves first-occurrence order. Each member retains its own business-response timestamp. Combined validation gates completion/drain and can fail the run; it does not replace all members' latency with the group completion time. Group results are materialized in the client and can consume memory proportional to their member results.

Snapshot queries wait for all pending write responses at/below their read point. Until then the single sender defers the query and continues with newer writes. Deferred queries leave in source order once their dependencies complete; later writes at/below a deferred read point wait for that query. `dependency_wait_seconds`, `search_deferral_seconds` and `deferred_search_operations` remain outside API latency and inside elapsed throughput time. The entire group must fit `max_in_flight` before its first member is admitted.

Latency mean/max retain measured nanoseconds; p50/p95/p99 are fixed-memory nearest-rank upper bounds, exact below 2048 ns and with less than 1/1024 relative quantization error thereafter. Measurements include the return path and client completion processing; they are not exact server-side visibility timestamps.

## Results and failure handling

Scans respect gRPC flow control and chunk at 16,384 results or approximately 1 MiB, whichever comes first. An individually larger result is sent alone, up to the 32 MiB message bound; oversized requests/results fail. Ordinary scans are counted without retaining all results. Partial results followed by RPC failure never constitute a successful query.

Replay checks native expected result counts, RPC status/completion, complete input consumption, input/source integrity, selected value mode and the harness binding. Equal counts do not certify matching keys, versions, values or order; production replay does not create per-query answer artifacts or a reference index. Independent acceptance checks those properties separately.

`request_bytes` sums serialized data RPC payloads, including batch envelopes. `result_bytes` sums all returned Result payloads, including each group member's rows before deduplication. Neither measures network bytes: response envelopes, repeated-field framing, metadata and transport overhead are excluded. `checked_result_items` counts the validated ordinary/group answers, which can differ from the number of Result items received over RPC.

## Internal persistence and reopening

Persistence belongs to the service. The adapter checks for completed writes every five seconds and persists dirty state, including after input becomes idle; there is no client phase barrier. A background persistence failure is retained, rejects subsequent calls and fails controlled shutdown. Request failures and later cleanup failures preserve the original error separately. Normal replay success certifies checked request completion and visibility, not remote durability of every acknowledged write.

Controlled shutdown cancels transports, waits up to 30 seconds at each transport/execution boundary, stops workers/maintenance and persists completed dirty writes. Native handles close only after workers and active persistence have exited; timeout/interruption keeps those handles valid until process exit. The underlying engine may also run its own WAL/flush/compaction maintenance.

`reopen: true` opens persisted data and checks value kind and Trace binding. `verify-service` runs the Trace's snapshot queries at their original read points and rejects a Trace with no queries to verify. It does not force persistence. Use a completed internal persistence cycle or controlled service shutdown before an explicit cold-reopen acceptance. Clean reopen does not certify crash recovery, cross-instance visibility or autoscaling. See [service commands](../README.md#replay-and-service).

## Local RocksDB fixture

All writes, including baseline entries, use local WAL without fsync, so write acknowledgment does not promise power-loss durability. Logical deletions retain versioned tombstones; no physical undo or version GC is implemented.

Local RocksDB remains a generation dependency and test fixture, outside the Index Service comparison. The comparison roster is RocksDB-Cloud and SlateDB.

## rocksdb-cloud backend

Build/loading instructions are in [README](../README.md#rocksdb-cloud-jni). The backend uses the same key, snapshot and ACK visibility semantics.

| Boundary | Behavior |
| --- | --- |
| Write acknowledgment | Memtable + local WAL without fsync for baseline and incremental writes. |
| Internal persistence | Uploads SST/MANIFEST; writes after the completed persistence cycle may exist only locally. |
| Cold start | `keep_local_sst_files=true`; missing local SSTs are downloaded. Unuploaded WAL is unrecoverable. |
| Deletion | No bucket creation; `run_purger=false`; ordinary expired/invisible file deletion defaults to a 1-hour delay. This is not logical version GC. |
| Statistics | Each internal persistence cycle writes `<index>.engine-stats.jsonl`; object-request metrics are N/A for local RocksDB. Both cloud backends count remote requests, failures, bytes and latency below any local copy. |

Object metrics include type, count, failures, bytes, cumulative microseconds and 32 histogram buckets with upper bounds 1, 2, 4…2^30 μs, then `Long.MAX_VALUE` (`overflow_after=2^30`). p50/p95/p99 are nearest-rank bucket upper bounds over successes and failures; no percentile is meaningful with zero samples. Concurrent snapshots are not transactional, so each histogram uses its own sample count. SDK-internal HTTP retries are not separately reported; parallel cumulative time is not wall time. Missing engine data is N/A; incompatible Java/JNI statistics formats fail at open.

Data-file Rewrite, physical version GC, unknown-outcome write retries and the complete source-snapshot lifecycle are unsupported. Reopening support does not by itself certify fault recovery or full-input capacity.

## SlateDB backend

[Build and loading](../README.md#slatedb-jni) use the pinned SlateDB source and a thin Rust JNI byte-store adapter. The shared Java implementation owns the same key layout and version selection as RocksDB-Cloud. All reads and writes use one `Db`, with memory-visible reads; no independent `DbReader` is opened.

Native writes return after applying to that Db's memory read path (`ACK_MEMORY`). Internal persistence waits for object-store persistence. Write completion and remote durability are separate boundaries. The configured S3 bucket must exist, and creation refuses an existing object prefix; an atomic owner marker also rejects concurrent creators. SlateDB's writer fencing and manifest publication rely on conditional object writes. Test them on the deployment in use; local S3 results do not certify COS. Tencent COS accepts but does not enforce `If-None-Match`/`If-Match` on PUT, so on COS the owner marker and fencing only hold for a single writer; runs there exercise the single-writer path and do not certify fencing.

Ordered cursors hold a SlateDB snapshot and stream keys in either direction, without copying a complete scan into Java; their data blocks enter the block cache like point reads. The local path holds SlateDB's object mirror: compacted SSTs are written to both the object prefix and local disk and read locally, matching RocksDB-Cloud's local SST copies, while WAL objects and manifests stay remote. The database itself resides in the configured object prefix; reopening uses that prefix, downloads missing mirrored SSTs and reads the persisted Java metadata.
