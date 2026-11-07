# Index Service contract

[index_service.proto](../proto/index_service.proto) defines namespace `sibench.index`. Driver and service are built together and consume the [canonical Trace](trace.md); no protocol-version negotiation is provided.

## Interfaces and acknowledgment boundaries

| RPC | Contract |
| --- | --- |
| `Describe` | Declares value type, versioned reads, query groups, baseline batching and acknowledgment/Flush durability. The driver rejects capabilities missing from the selected input. |
| `Execute` | One bidirectional stream per client. Initial `sibench-index-binding` metadata binds Trace, value mode and secondary-index selection before requests are accepted. IDs are unique per stream; responses correlate by ID and may arrive out of order. A successful mutation response follows application to the normal read path. |
| `Flush` | Drains accepted requests, persists data and the Trace binding at declared durability, and returns the index location. It is a phase boundary, not a per-query visibility repair. |

`Describe` uses `replay.request_timeout_seconds`; client Flush and server drain deadlines are 3600 seconds. Any failed accepted request makes Flush fail. The driver checks the returned binding and nonempty location.

Local send acknowledgment means serialization and transport handoff, not remote receipt/execution; enqueueing alone is insufficient. Checked responses complete separately. Buffered batch/group members and RPC counts are distinct. `INVALID` denotes a contract violation; `UNAVAILABLE` denotes execution failure with potentially unknown write outcome. Such writes are not automatically retried. Preserve the first error separately from drain/cleanup failures.

## Measurement

Replay times every basic write and lookup from entry to the Index Client API through completion of its final business response, using the replay process's monotonic clock. Client serialization and batch/group buffering are included; replay admission waits at the in-flight limit are reported separately as backpressure. Neither stream enqueueing nor the local transport callback ends response latency. A chunked lookup ends only at its final response. Streaming avoids repeated RPC setup; it does not remove queueing, flow control or scheduling variability.

Each write entry and each SearchGroup member counts as one basic operation. Trace source boundaries are not sent to the service and do not enter warmup, throughput or latency statistics. `target_calls` counts basic operations; `execute_requests` counts protocol messages, including batches/groups, within the persistent streams. Returned items and serialized bytes have separate counters. Warmup membership follows each operation through its response, including shared batch completions. Write/search histograms exclude warmup operations.

Before admitting a snapshot query, replay waits for pending write responses whose versions are at or below its `read_version`. For a group, this happens before its first member. Older snapshots may proceed while newer writes are pending. This preserves the source snapshot without adding a server-side transaction protocol. `dependency_wait_seconds` reports this replay wait separately from API latency; it remains part of elapsed throughput time.

Before sending a SearchGroup's first member, replay waits for capacity for the whole group. `max_in_flight` must accommodate the largest included group; an oversized group fails before any of its members is sent. This prevents buffered members from exhausting the limit while waiting for the still-unsent remainder.

Latency mean/max retain measured nanoseconds; p50/p95/p99 use fixed-memory nearest-rank upper-bound buckets, exact below 2048 ns and with less than 1/1024 relative quantization error thereafter. Values are reported in milliseconds without rounding down to whole milliseconds. Failed/timed-out requests fail the run and have separate counters; they are not successful latency samples.

Client response latency includes the response return path and client completion handling. It is a completion measurement, not an exact server-side visibility timestamp. No polling, sampled lookups or independent reader is added to ordinary replay; the workload's own lookups remain measured.

Shutdown cancels transports and waits up to 30 seconds for each transport/execution shutdown boundary. Native backend handles are closed only after execution workers and an active Flush have exited; a timeout or interruption fails shutdown and leaves those handles open until process exit.

## Data and snapshot reads

An entry is `(table, index, indexed_value, PK)`; primary index 0 has no indexed value. Components use the [Trace encoding](trace.md#identity-and-versions). Values are RowID or Locator `(file, position)` with two full int64 fields; capability is checked per mode.

The service consumes committed updates and committed-snapshot queries. Generation excludes aborted writes and transaction-local searches, and keeps only the final write to each physical index identity within a commit version. There are no Commit/Abort RPCs, provisional versions or transaction-local overlays. Source transaction boundaries remain in Trace for provenance and input validation only.

| Operation | Contract |
| --- | --- |
| Baseline `INSERT` | Version 0, no WAL; declared durability only after Flush. A `WriteBatch` is validated entirely before application. Accounting/latency stays per member and a shared completion uses the earliest deadline. |
| `UPSERT` / `LOGICAL_DELETE` | Applies one committed-version entry. A successful response means it is visible to normal queries at applicable read versions. A selected deletion hides older values. |
| `RELOCATE` | Replaces the location of an existing exact version without creating a logical version; not a complete Rewrite/GC workflow. |
| `Search` | Latest version at/below `read_version`, excluding selected deletions. Supports primary/secondary bounds, inclusivity, direction, limit and secondary entries pinned to a PK. Order is indexed value then PK, reversed when descending. |
| `SearchGroup` | Intersects/unions members by PK. Members share table, read point and group identity. Intersection follows the first member's order; union uses first-occurrence order. |

Successful mutation ACK guarantees visibility through the same service instance, including requests from another client stream. It does not promise remote durability; the backend declares that boundary separately. The driver supplies each physical key/version once and establishes query dependencies before dispatch. The service does not infer whether a source transaction has completed or hold reads for missing source versions.

## Results and validation

Ordinary queries stream chunks of 16,384 results under gRPC flow control. `more=true` means another chunk for the same ID; only the last carries final status. The driver counts chunks without retaining the entire ordinary result. Groups are fully materialized and may require memory proportional to member results.

Replay requires matching native expected counts, valid status/chunk completion, complete request consumption, input/source integrity, selected capabilities and successful Flush with the matching binding. Capability requirements derive from verified selected requests, not catalog statistics. Equal counts do not establish equal keys, versions, values or order; no per-query answer artifact/reference index is created by production replay.

`request_bytes` sums serialized Request sizes, including batches. `result_bytes` sums serialized Result sizes after completed responses pass count checks. Neither measures network bytes: response envelopes, repeated-field framing, empty responses, transport overhead and retries are excluded.

## Persistence and reopening

Flush covers requests accepted before the call began and stores the binding with data. Another Trace, value mode or index selection is rejected. The report's index manifest records input/build/configuration identity, source version coverage, encoding and backend location. `last_source_version` is derived from the consumed Trace, not a service publication watermark.

`reopen: true` opens persisted data with the matching binding and value kind. Unsupported reserved metadata from older transactional indexes is rejected; use a fresh index for this contract. `verify-service` checks exported snapshot queries at their original read points. See [service commands](../README.md#replay-and-service).

## Local RocksDB fixture

Baseline bypasses WAL; other writes use local WAL without fsync, so write acknowledgment does not promise power-loss durability. Logical deletions retain versioned tombstones; no physical undo or version GC is implemented.

Local RocksDB remains a generation dependency and test fixture, outside the Index Service comparison. The comparison roster is RocksDB-Cloud and SlateDB.

## rocksdb-cloud backend

Build/loading instructions are in [README](../README.md#rocksdb-cloud-jni). The backend uses the same key, snapshot and ACK visibility semantics.

| Boundary | Behavior |
| --- | --- |
| Write acknowledgment | Memtable + local WAL without fsync; baseline bypasses WAL. |
| Flush | SST/MANIFEST uploads acknowledged; remote durability through that Flush. Later writes may exist only locally. |
| Cold start | `keep_local_sst_files=true`; missing local SSTs are downloaded. Unuploaded WAL is unrecoverable. |
| Deletion | No bucket creation; `run_purger=false`; ordinary expired/invisible file deletion defaults to a 1-hour delay. This is not logical version GC. |
| Statistics | Each Flush writes `<index>.engine-stats.jsonl`; object-request metrics are N/A for local RocksDB. |

Object metrics include type, count, failures, bytes, cumulative microseconds and 32 histogram buckets with upper bounds 1, 2, 4…2^30 μs, then `Long.MAX_VALUE` (`overflow_after=2^30`). p50/p95/p99 are nearest-rank bucket upper bounds over successes and failures; no percentile is meaningful with zero samples. Concurrent snapshots are not transactional, so each histogram uses its own sample count. SDK-internal HTTP retries are not separately reported; parallel cumulative time is not wall time. Missing engine data is N/A; incompatible Java/JNI statistics formats fail at open.

Data-file Rewrite, physical version GC, unknown-outcome write retries and the complete source-snapshot lifecycle are unsupported. Reopening support does not by itself certify fault recovery or full-input capacity.

## SlateDB backend

[Build and loading](../README.md#slatedb-jni) use the pinned SlateDB source and a thin Rust JNI byte-store adapter. The shared Java implementation owns the same key layout and version selection as RocksDB-Cloud. All reads and writes use one `Db`, with memory-visible reads; no independent `DbReader` is opened.

Native writes return after applying to that Db's memory read path (`ACK_MEMORY`). Flush waits for object-store persistence. Write completion and remote durability are separate boundaries. The configured S3 bucket must exist, and creation refuses an existing object prefix; an atomic owner marker also rejects concurrent creators. The object store must support the conditional publication operations required by SlateDB. Test these on the deployment in use; local S3 results do not certify COS.

Ordered cursors hold a SlateDB snapshot and stream keys in either direction, without copying a complete scan into Java. Object-request metrics are currently unavailable and reported as N/A, not zero. The local path holds diagnostics; the database itself resides in the configured object prefix. Reopening uses that prefix and the persisted Java metadata.
