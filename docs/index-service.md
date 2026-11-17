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

## Persistence and reopening

The service checks for dirty state every five seconds and persists completed writes, also after input goes idle; this is not a per-write deadline. A partial write failure can leave dirty state. A persistence failure rejects later calls and fails controlled shutdown, and cleanup keeps the original request error.

Controlled shutdown waits at most 30 seconds each for transport and execution, stops maintenance and persists dirty writes. Native handles stay valid until workers and persistence exit; a timeout never closes active handles. A successful replay establishes request completion and visibility, not durability of every ACK.

`service.reopen: true` opens persisted state and validates binding and value kind. `verify-service CONFIG BENCHMARK REPORT` re-checks the original snapshot queries, rejects query-free Traces and does not force persistence; reopen after a completed persistence cycle or controlled shutdown. A clean reopen does not establish crash recovery, cross-instance visibility or autoscaling.

## Backends

The comparison targets are RocksDB-Cloud and SlateDB; local RocksDB is a generation and test fixture. [Build commands](../README.md#build) use pinned sources and fresh outputs. Physical version GC and unknown-outcome retry are unsupported.

| Backend | Write ACK | Persistence and storage |
| --- | --- | --- |
| RocksDB-Cloud | `service.wal: false`: memory (`ACK_MEMORY`); `true`: local WAL without fsync (`ACK_WAL`). | Persistence flushes and uploads SSTs and MANIFEST. Writes after the last persistence can be lost, and an unuploaded WAL is unrecoverable. Local SST copies are kept; missing ones download on reopen. |
| SlateDB | Memory-visible (`ACK_MEMORY`) under either WAL setting. | With WAL, persistence waits for the uploaded WAL; without it, an L0 SST is written. A clean close flushes either mode. SSTs have local mirrors; WAL and manifests stay remote. |
| Local RocksDB | Local WAL without fsync. | No power-loss durability promise; historical tombstones are kept. |

`index-server CONFIG SERVICE_CONFIG` resolves the backend's `service.storage` URI through CONFIG's object-storage connections ([selection and storage](trace.md#selection-and-storage)). `service.wal` is required, applies to baseline and workload writes alike, and is false in both cloud templates. Buckets must exist.

SlateDB refuses existing prefixes on creation and depends on conditional object writes for ownership, fencing and publication; verify them on the actual deployment. On endpoints that ignore PUT conditions the adapter serializes and checks conditional PUTs within one process only, so **run exactly one SlateDB service process per object prefix**. This cannot fence other processes or certify durability.

Cloud backends write engine and remote-request statistics (below the local copies) to `<index>.engine-stats.jsonl`; missing data is N/A. Object latency percentiles are nearest-rank bucket upper bounds over successes and failures. Cumulative parallel latency is not wall time, and SDK-internal retries are not counted separately. A successful build or reopen does not certify full-input capacity or recovery.

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

`ServiceAcceptanceTest` runs both commands against configured non-Relive gRPC services, including a wrong value with the correct count; `ServiceValueCheckTest` keeps the wrong-value, ordering, framing, partial-write and incomplete-stream counterexamples.

## Relive bridge

The [Relive HTTP service](../index/relive/README.md) runs on the same host as the SIBench gRPC bridge. Relive owns the CFS journal, L0 and metadata and the authoritative COS L1; the bridge keeps no index files or disk cache. Put this service configuration in your deployment directory and start it with `index-server CONFIG SERVICE_CONFIG`:

```yaml
schema_version: 1
purpose: index_service
service:
  backend: relive
  endpoint: http://127.0.0.1:7001
  port: 50551
  value: row_id
```

Replay targets the bridge's gRPC endpoint like any other service. The bridge reuses the existing request validation, private-write merge, binding metadata, flow control and completion checks, and preserves each write's `ts` and each read's `read_ts`. Private writes stay in the Java request context and never reach Relive's write endpoint; ReadGroup composition and snapshot ACK dependencies stay in the replay client. No source transaction is re-executed.

**Encoding.** A key is the lowercase hex of `00 ++ full opaque key`. The fixed prefix keeps unsigned order, prefix keys and the empty key without changing Relive's nonempty-key protocol, and needs a fresh store. Values are plain hex, with null reserved for a tombstone. Keys are limited to 65,535 bytes and values to 65,536 bytes, and larger input fails. A batch is validated in full, then split at Relive's 10,000-row/8 MiB JSON limits; a failed later chunk can leave earlier chunks applied, and nothing is retried.

**Reads.** Reads use the streaming HTTP endpoint and require its header, ordered in-range entries, matching terminal counts and a normal EOF. Missing completion, malformed or trailing output fails the RPC even after partial values. Each HTTP exchange has a 60-second absolute deadline, stream bodies included; an overlong scan fails and is never truncated or retried. The bridge checks representation and completion only, so value correctness still needs an independent oracle.

**Binding.** A persistent binding holds the Trace binding and value mode. A headerless caller gets a reserved ordinary-caller binding on first access, and a populated ordinary store cannot later accept another Trace; a Trace-bound store needs its original binding. The bridge caches the accepted binding and description, so keep its HTTP target on the same store: moving to another index needs a bridge restart and a new binding, and live retargeting is unsupported.

**Durability.** The description reports `ACK_WAL` with `shared_nfs_fsync` durability. An HTTP write returns after the shared journal is forced, the journal byte boundary is published atomically and its directory forced, and the write is visible. Bridge flush forces neither L1 upload nor maintenance drain. Count the submitted writes, stop new submissions, and measure maintenance separately from Relive's [maintenance statistics](../index/relive/docs/local-protocol.md); a full compaction can rewrite stable L1 and overlap background work, so its duration is not a drain measurement. Compare this ACK only with equivalent guarantees, or report it separately from memory ACKs. The reported location names the engine's CFS directory and object-storage identity. CFS durability and cross-host visibility need deployment acceptance.

The default tests use an HTTP boundary fixture. To check the real engine, HTTP and gRPC path with hand-derived historical values (not CFS/COS acceptance, full-Trace validation or a performance run), package Relive and run its integration check:

```bash
mvn -f index/relive/pom.xml -Drelive.build.directory="$PWD/build/index/relive" package
mvn -Dtest=ReliveIntegrationCheck \
  -Drelive.test.jar="$PWD/build/index/relive/relive.jar" \
  -Drelive.test.classes="$PWD/build/index/relive/test-classes" test
```

Explicit cloud checks use the same test classpath (SIBench JAR, test classes, JUnit, Hamcrest) and fresh engine state with explicit memory and process budgets. Bound process shutdown separately, and keep Java temporary files and run evidence on cloud storage.

| Entry point | Check |
| --- | --- |
| `ReliveIntegrationCheck populate HTTP_A HTTP_B` | The common oracle through two bridges to two Relive processes sharing one fresh CFS directory and COS prefix (300 s). |
| `ReliveIntegrationCheck reopen HTTP` | After stopping both processes, retained history, empty values, binding and private-write isolation (120 s). |
| `ReliveIntegrationCheck populate-journal HTTP_A HTTP_B` | The same oracle without explicit flush or compaction, on a separate fresh state with at least a 256 KiB Memtable per engine; it needs zero flushed files, positive memory and journal bytes and no uploaded L1 bytes. Kill both engines without graceful shutdown, reopen with an empty process cache and run `reopen` to test the acknowledged journal tail. Record Stats before the kill and after reopening. |
| `ReliveLoadCheck HTTP_WRITER HTTP_OBSERVER CLIENTS FRESH_REPORT [MUTATIONS]` | 1 or 4 write clients, 64–8,192 mutations in multiples of 64 (default 64) and half as many snapshot-zero Gets. Versions are fixed before submission and delivered in descending order over eight keys, keeping deletion and reinsertion histories. After all write ACKs it observes another instance, flushes, waits for both instances' background maintenance to be idle and checks old, middle and latest snapshots, without a final compaction. Deadlines: 600 s workload, 30 s per data RPC, 120 s drain. |

These are single-host functional and diagnostic checks. The process-loss check does not certify host power loss, storage-server failure or cross-host durability. `ReliveLoadCheck` reports the ACK-to-observer interval as an upper bound on index visibility ending at one correct Scan, not a lakehouse commit metric. Its last-write-submission and last-write-ACK to observed-idle fields include remaining foreground reads and the polling interval, so they do not timestamp the instant background work became idle. Neither check establishes sustained throughput or full-Trace acceptance; use the normal replay and verification workflow for selected Traces.
