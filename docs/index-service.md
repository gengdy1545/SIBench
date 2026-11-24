# Index Service contract

[index_service.proto](../proto/index_service.proto) defines five RPCs in `sibench.index`. Keys and values are opaque bytes, including empty values and prefix-related keys. SIBench owns business-key encoding and RowID/Locator conversion. Client, service and persisted indexes share one representation; a change needs matching clients and fresh indexes.

## Data interfaces

| RPC | Contract |
| --- | --- |
| Get | Exact key at `read_ts`; returns an optional value. Absent and present-empty are distinct. |
| Put | Key, caller-assigned `ts` and value. ACK follows same-instance visible application. |
| Delete | Key and `ts`. A selected tombstone hides older values without invalidating historical reads. |
| Scan | Unsigned byte range `[start_key,end_key)` at `read_ts`; omitted endpoints are unbounded. Ascending unless reversed; `limit=0` is unlimited. Returns one value per visible key in key order, keeping duplicates. |
| WriteBatch | Put/Delete members, validated before application; an empty batch fails. ACK follows every member. A failure may be partial; atomic visibility and rollback are not promised. |

Scan is server-streaming and the others are unary. Reads return values only and select the newest version at or below `read_ts`. Timestamps and limits must fit a nonnegative int64. Timestamp 0 uses the ordinary ACK policy. The service allocates no revisions and has no transaction, Describe or Flush RPC.

A read may carry ordered, unique-key `local_writes` inside its bounds, with timestamp zero. Private Puts replace snapshot values and Deletes hide them; Scan merges by key before applying direction and limit. Contexts are request-scoped, never persisted or shared, and limited to 1,000,000 entries and 32 MiB. SIBench's shared Java layer supplies this visibility over each backend; it does not certify native backend transactions. [Trace](trace.md#operations-and-index-expansion) defines source commits and private contexts.

An ACK guarantees visibility to other clients of the same instance, not remote durability. Conflicting writes at one key and timestamp are unsupported. INVALID_ARGUMENT rejects invalid input. Cancellation, deadlines and UNAVAILABLE may leave a write outcome unknown, and the driver never retries them. A local transport handoff proves neither receipt nor execution.

## Harness identity and deployment description

Replay attaches `sibench-index-binding` (the workload and baseline Trace identities plus the secondary-index selection) and `sibench-value-kind` metadata. The server binds a fresh index to its first binding and refuses mismatches; it compares the value kind with its own and persists it with the index. Ordinary callers may omit the headers; they are guards, not authentication.

The server returns a JSON IndexDescription in binary `sibench-service-bin` metadata: backend, value mode, ACK level, WAL setting, durability and storage location. Replay validates the first response's description, and every later response, including empty Scans, must repeat its exact bytes. Reports bind input, build, configuration and backend location. `last_source_version` describes consumed input, not a durability watermark.

## Replay and measurement

Replay batches baseline Puts by `baseline_batch_operations`; every incremental write or read member is its own RPC. All dispatch settings in [config/sibench.yaml](../config/sibench.yaml) are required, and `client_count × (batch − 1)` must stay below `max_in_flight`. Baseline reports separate `operations`, `rpc_requests` and batch size; a buffered member counts as sent when batched, before transport handoff.

After the baseline validates and all its write ACKs arrive, `replay-service` writes `REPORT.baseline.json` (status `baseline_passed`) with the input/build binding, service description, dispatch configuration and baseline measurements. It survives a later workload failure and certifies neither workload completion, maintenance drain nor verified stored values. A failed or interrupted baseline writes none. Use fresh report paths; neither file authorizes resuming a replay.

ReadGroup is a Trace description, not an RPC. The client combines member values [as Trace defines](trace.md#operations-and-index-expansion) and checks the combined expected count. Group results can use memory proportional to their members, and a whole group must fit `max_in_flight` before admission.

Snapshot queries wait for pending write ACKs at or below their read point. The sender may defer them while sending newer writes; deferred queries leave in source order, and later writes at or below a deferred read point wait. Dependency and deferral waits and admission backpressure are outside API latency but inside elapsed throughput time.

Latency runs from client API entry to the operation's final response, including serialization and buffering; a Scan ends at normal stream completion. Each group member keeps its own completion time, and combined validation gates the drain. Baseline and workload are separate phases. Warmup membership follows input order, and warmup and failed requests are excluded from successful latency samples. Source markers have no call or latency.

Mean and max keep nanoseconds. p50/p95/p99 are nearest-rank upper bounds, exact below 2048 ns and within 1/1024 relative quantization error above. Client completion is not an exact server-visibility timestamp. Background persistence and maintenance count toward experiment resource cost.

## Results and failure handling

Scans respect flow control and chunk at 16,384 values or about 1 MiB; a larger value may travel alone up to the 32 MiB message bound, and oversized payloads fail. Partial results followed by an RPC failure never count as success.

Replay checks input and source integrity, full consumption, binding and value mode, status and completion, and native expected counts. **Matching counts do not certify values or order.** Ordinary replay builds no reference index or per-query answer artifact; [independent acceptance](#independent-acceptance) compares values, since the value-only API cannot expose selected keys or timestamps.

| Report field | Unit |
| --- | --- |
| `target_calls` | Basic operations, including individual baseline batch members. |
| `rpc_requests`, `rpc_by_kind` | Actual RPCs by method. |
| `request_bytes` | Serialized request payloads, including batch envelopes and private contexts. |
| `result_bytes` | Value bytes received, including group members before deduplication. |
| `checked_result_items` | Validated ordinary or combined-group results. |
| `private_context_reads`, `private_context_entries` | Dispatched read members with contexts, and their entries. |

Byte counts exclude transport overhead (and response envelopes for results); they are not network-byte measurements.

## Service execution and statistics

Every `SERVICE_CONFIG` requires an `execution` mapping alongside `service`:

```yaml
execution:
  read_threads: 16
  write_threads: 8
  statistics: build/services/index/run.stats.jsonl
```

These positive thread counts set SIBench's read and write worker pools for every backend. Synchronous bindings occupy a worker until they return; asynchronous writes release it after submission and complete the RPC only when the backend future succeeds. `write_threads` therefore limits submission workers, not outstanding asynchronous writes. The replay client's `max_in_flight` controls its outstanding requests; there is no separate service-wide admission limit for unrelated clients. Size these budgets for the deployment, including concurrent Scan buffering and downstream flow control.

Replay admission and gRPC calls are common to all backends. Backend adapters translate those calls into the backend's API; engine worker pools, caches, compaction and storage scheduling remain engine settings. An engine's worker count does not determine the service's worker budget. Increasing SIBench concurrency alone does not establish higher engine or end-to-end throughput.

SIBench creates `execution.statistics` as a fresh JSONL file, relative to the project root unless absolute. Use a new path for each service start, including reopening an index; an existing file is rejected. Each persistence check (about every five seconds, also without writes) and drained shutdown writes a cumulative sample containing `time_millis`, `backend` (deployment description), `execution` (thread counts), `service` (common call timings) and `engine` (optional backend measurements). Statistics output failure rejects subsequent calls and fails controlled shutdown, preserving an earlier persistence error if present.

`service.read_queue` and `write_queue` measure submission-to-worker wait; `read_execute` and `write_execute` measure worker start through backend completion, including adapter and network waits. `flush_call` measures explicit persistence calls, including failures. Each contains request/failure counts, cumulative microseconds and histogram percentile upper bounds; `bytes` is unused for service calls. Cumulative parallel time is not wall time or engine CPU time. Concurrent samples are approximate; use the final drained sample for complete counts. These timings are separate from replay's client API-to-response latency.

`engine` may be empty. The cloud bindings expose RocksDB properties or SlateDB's `slatedb.metrics` text and remote-request counters below local copies. Remote latency percentiles include successes and failures; SDK-internal retries are not separate requests. Missing engine measurements are N/A, while common service measurements remain available for every backend.

## Persistence and reopening

The service checks for dirty state every five seconds and persists completed writes, also after input goes idle; this is not a per-write deadline. A partial write failure can leave dirty state. A persistence failure rejects later calls and fails controlled shutdown, and cleanup keeps the original request error.

Controlled shutdown waits at most 30 seconds each for transport and execution, including pending asynchronous storage calls, stops maintenance and persists dirty writes. Backend handles stay valid until workers, storage calls and persistence exit; a timeout never closes active handles. A successful replay establishes request completion and visibility, not durability of every ACK.

`service.reopen: true` opens persisted state and validates binding and value kind. `verify-service CONFIG BENCHMARK REPORT` re-checks the original snapshot queries, rejects query-free Traces and does not force persistence; reopen after a completed persistence cycle or controlled shutdown. A clean reopen does not establish crash recovery, cross-instance visibility or autoscaling.

## Backends

The comparison targets are RocksDB-Cloud and SlateDB; local RocksDB is a generation and test fixture. [Build commands](../README.md#build) use pinned sources and fresh outputs. Physical version GC and unknown-outcome retry are unsupported.

Both cloud bindings fix the same comparison settings in their native open path: LZ4 block compression, a 1 GiB data block cache (metadata caching stays engine-specific) and a limit of four concurrent compactions. RocksDB-Cloud (`max_background_jobs=5`) runs one flush thread and only one compaction until L0 reaches eight files or writes stall; SlateDB's compactor admits up to four at any time. SlateDB's async runtime uses one worker per available core. A Get or version lookup reads through a cursor bounded to that key's versions.

| Backend | Write ACK | Persistence and storage |
| --- | --- | --- |
| RocksDB-Cloud | `service.wal: false`: memory (`ACK_MEMORY`); `true`: local WAL without fsync (`ACK_WAL`). | Persistence flushes and uploads SSTs and MANIFEST. Writes after the last persistence can be lost, and an unuploaded WAL is unrecoverable. Local SST copies are kept; missing ones download on reopen. |
| SlateDB | Memory-visible (`ACK_MEMORY`) under either WAL setting. | With WAL, persistence waits for the uploaded WAL; without it, an L0 SST is written. A clean close flushes either mode. SSTs have local mirrors; WAL and manifests stay remote. |
| Local RocksDB | Local WAL without fsync. | No power-loss durability promise; historical tombstones are kept. |

`index-server CONFIG SERVICE_CONFIG` resolves the backend's `service.storage` URI through CONFIG's object-storage connections ([selection and storage](trace.md#selection-and-storage)). `service.wal` is required, applies to baseline and workload writes alike, and is false in both cloud templates. Buckets must exist.

SlateDB refuses existing prefixes on creation and depends on conditional object writes for ownership, fencing and publication; verify them on the actual deployment. On endpoints that ignore PUT conditions the adapter serializes and checks conditional PUTs within one process only, so **run exactly one SlateDB service process per object prefix**. This cannot fence other processes or certify durability.

A successful build or reopen does not certify full-input capacity or recovery. [Service statistics](#service-execution-and-statistics) keep common call measurements separate from engine properties.

## Independent acceptance

All deployed backends share the [data protocol](../proto/index_service.proto), Trace requests and timestamp semantics. The run's service is the gRPC target in the runtime CONFIG:

```yaml
target:
  secondary_indexes:
    enabled: false
  service:
    endpoint: 127.0.0.1:50551
    value: row_id
```

`target.service` holds only `endpoint` and `value`. `endpoint` is a gRPC target, not a backend HTTP URL. Engine, storage and WAL settings stay in the server configuration. Keep each run's effective CONFIG and service description, and keep variants outside `config/`.

These explicit checks sit outside normal generation and performance replay. Build the application and tests ([prerequisites](../README.md#build)), then:

```sh
mvn dependency:build-classpath -Dmdep.outputFile=build/sibench/test-classpath.txt
CHECK_CP="build/sibench/test-classes:build/sibench/sibench.jar:$(cat build/sibench/test-classpath.txt)"
java -cp "$CHECK_CP" org.sibench.adapters.IndexServiceCheck CONFIG REPORT
java -cp "$CHECK_CP" org.sibench.adapters.ServiceValueCheck CONFIG BENCHMARK FRESH_REFERENCE REPORT
```

Use a fresh service state and report path for each command. Neither command resumes or overwrites.

| Check | What it verifies |
| --- | --- |
| `IndexServiceCheck` | Hand-derived cases on one configured instance with several callers: historical values, out-of-order versions, delete/reinsert, empty keys and values, ordered and reversed scans, private overlays. Cross-instance visibility, compaction, recovery and maintenance APIs are separate deployment checks. The fixed cases have a 300-second RPC budget. |
| `ServiceValueCheck` | Ordinary Trace replay (same reader, projection, `ts`/`read_ts`, dependencies, value mode and binding; baseline WriteBatch boundaries preserved) against the configured endpoint, with acknowledged writes mirrored into a fresh versioned RocksDB reference. Get compares exact values (absence differs from empty); Scan compares the count and an ordered, length-framed SHA-256 digest, keeping duplicates and empty values. A failure stops validation without retry. |

Setup issues one empty-range Scan to bind the index and read the service description; it is outside the Trace operation counts. `ServiceValueCheck` writes the ordinary replay report with its baseline and index sidecars, plus `REPORT.values.json` (effective configuration, endpoint, binding, check counts, timing, failures). Acceptance needs both reports to pass with the expected input coverage; a failure can leave a completed baseline report without claiming the workload finished. Process termination can still prevent report publication.

Choose reference, temporary and report paths by the deployment's resource budget, and account separately for the RocksDB JNI extraction into `java.io.tmpdir`, reference memory and disk, both services, setup and shutdown. `call_timing` groups Put/Delete/WriteBatch as `write` and records Get/Scan calls for the actual and reference implementations; concurrent durations overlap and the actual Scan includes downstream delivery. These are validation costs, not backend throughput or client ACK latency. The reference shares request and private-write helpers with the client, so the hand-derived cases remain necessary, and a small run does not establish full-Trace capacity.

`ServiceAcceptanceTest` runs both commands against configured gRPC services, including a wrong value with the correct count; `ServiceValueCheckTest` keeps the wrong-value, ordering, framing, partial-write and incomplete-stream counterexamples.

## Relive

The [Relive client](../index/relive/README.md) runs embedded in the index server; there is no separate Relive process. Build and install it before building SIBench, as shown in the [build instructions](../README.md#build), and repeat that step after changing Relive. Select it with [relive.yaml](../config/index/relive.yaml):

```yaml
service:
  backend: relive
  client: config/index/relive-client.yaml
  clients: 8
  port: 50551
  value: row_id
```

`client` names the [Relive client configuration](../config/index/relive-client.yaml), relative to the project root: a fresh CFS directory and DynamoDB metadata namespace per run. `clients` opens that many independent Relive clients, each with its own metadata connection and manifest cache; requests rotate among them. The index has one shard.

Keys are the hex of `00 ++ key`, which keeps unsigned order and represents the empty key; key `01` at timestamp 0 holds the Trace binding, so a populated index refuses another Trace. A batch is split at 10,000 rows and about 7 MiB; each part commits separately. The adapter does not resubmit failed writes; the client handles metadata contention within its publication attempt budget. Writes run on the SIBench write workers until durable; private writes stay in the request context.

Keys are limited to 65,535 bytes and values to 65,536 bytes. Oversized entries fail; a failed later part can leave earlier parts committed. Callers without binding metadata reserve `ordinary-callers` on first access, including their selected value mode; that index then rejects a different Trace binding.

The description reports `ACK_WAL` with `cfs_fsync_metadata_commit` durability: the write's file and directory were forced on CFS and its metadata commit succeeded. `flush` does nothing. Relive's separate compactor currently reports backlog only; compaction, cold-data upload and reclamation are not implemented.

`ReliveBackendCheck` covers history, deletion, reinsertion, binding, private writes and the common gRPC value oracle across independent backends and reopening. It requires a writable CFS directory, a running DynamoDB endpoint and the credential environment variables named in the client configuration. This check does not establish cross-host visibility, full-Trace correctness or performance:

```bash
mvn -Dtest=ReliveBackendCheck -Dsibench.relive.cfs=/mnt/cfs/DIR -Dsibench.relive.dynamodb=http://127.0.0.1:8000 test
```
