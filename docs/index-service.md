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

After complete baseline validation and all its write ACKs, `replay-service`
writes `REPORT.baseline.json` with status `baseline_passed`, the input/build
binding, service description, dispatch configuration and baseline measurements.
It remains available if the workload later fails. It does not certify workload
completion, maintenance drain or independently verified stored values. A failed
or interrupted baseline creates no such report. Use a fresh report path and
sidecar; neither artifact authorizes resuming an interrupted replay.

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

## Independent acceptance

All deployed backends use the same [data protocol](../proto/index_service.proto),
Trace requests and read/write timestamp semantics. Select the service for each
run in the runtime CONFIG:

```yaml
target:
  secondary_indexes:
    enabled: false
  service:
    adapter: grpc
    endpoint: 127.0.0.1:50551
    value: row_id
```

The endpoint is a gRPC target, not a backend HTTP URL. Service deployment settings
(engine, storage and WAL policy) remain in the server configuration. Preserve each
run's effective CONFIG and service description. Keep experiment variants outside
the public configuration directory. Relive's HTTP bridge is one implementation of
this endpoint; the acceptance commands do not select a backend by name.

These explicit checks are outside normal generation and performance replay. Build
matching application/test artifacts using the [build prerequisites](../README.md#build),
then obtain the test dependency classpath:

```sh
mvn dependency:build-classpath -Dmdep.outputFile=build/sibench/test-classpath.txt
CHECK_CP="build/sibench/test-classes:build/sibench/sibench.jar:$(cat build/sibench/test-classpath.txt)"
java -cp "$CHECK_CP" org.sibench.adapters.IndexServiceCheck CONFIG REPORT
java -cp "$CHECK_CP" org.sibench.adapters.ServiceValueCheck CONFIG BENCHMARK FRESH_REFERENCE REPORT
```

Run the commands on separate fresh service states and fresh report paths.
`IndexServiceCheck` uses hand-derived historical values, out-of-order versions,
delete/reinsert, empty keys/values, ordered/reversed scans and private overlays.
It checks multiple callers of one configured service instance. It does not require
cross-instance visibility, compaction, recovery or a backend maintenance API.
Those remain separate deployment checks. Its fixed cases have a 300-second RPC
budget at the local test service, with bounded concurrency waits; outbound calls
use CONFIG's `replay.request_timeout_seconds`.

`ServiceValueCheck` runs the ordinary Trace replay through a temporary local
checking service and the configured gRPC endpoint. It uses the same Trace reader,
operation projection, source `ts`/`read_ts`, dispatch dependencies, value mode and
binding as ordinary replay. Put/Delete remain their respective RPCs; baseline
WriteBatch boundaries are preserved. It mirrors acknowledged writes to a fresh
versioned RocksDB reference. Get compares exact values, including absence versus
empty; Scan compares count and an ordered, length-framed SHA-256 digest, preserving
duplicates and empty values. Failure stops validation without automatic retry;
partial Scan output cannot make a failed RPC successful.

Both commands reuse the ordinary gRPC transport's metadata, deadline, completion
and no-retry checks. During setup, an empty-range Scan establishes the intended
binding and obtains the service description without reading entries. This setup
call is outside the Trace operation counts. No Describe or Flush RPC is added;
remote persistence and maintenance remain the deployment's responsibility.

The value audit writes the ordinary replay report and its baseline/index sidecars,
plus `REPORT.values.json` containing the effective configuration, endpoint, binding,
check counts, timing and failure information. Successful acceptance requires both
reports to pass with the expected input coverage. Failure can retain a completed
baseline report without claiming workload completion. Fresh-path checks reject
existing references and reports; there is no resume or overwrite mode. Close is
controlled by the command, so no separate bridge shutdown is needed to save its
check report. Process termination can still prevent report publication.

Choose reference, temporary and report paths according to the deployment's resource
budget; the generic checker imposes no CFS/NFS requirement. Account for the pinned
RocksDB JNI extraction into `java.io.tmpdir`, reference memory/disk, both services
and setup/shutdown separately. The report's `call_timing` groups Put/Delete/WriteBatch
under `write` and records Get/Scan calls for actual and reference implementations.
Concurrent durations overlap; actual Scan includes downstream delivery. These are
validation costs, not isolated backend throughput or client ACK latency. The
reference shares request/private-write helpers, so the hand-derived protocol cases
remain necessary. A successful small check does not establish full-Trace capacity.

`ServiceAcceptanceTest` exercises both commands against configured non-Relive gRPC
services, including a deliberately wrong value with the correct result count.
`ServiceValueCheckTest` retains the wrong-value, ordering, framing, partial-write
and incomplete-stream counterexamples.

## Relive bridge

Start the [Relive HTTP service](../index/relive/README.md) on the same host as the
SIBench gRPC bridge. Relive owns CFS journal/L0/metadata and authoritative COS L1;
the bridge holds no index files or disk cache. Put this service configuration in
your deployment directory, then run `index-server CONFIG SERVICE_CONFIG` as above:

```yaml
schema_version: 1
purpose: index_service
service:
  backend: relive
  endpoint: http://127.0.0.1:7001
  port: 50551
  value: row_id
```

Replay still selects `adapter: grpc`. The bridge reuses the existing request
validation, private-write merge, binding metadata, flow control and result
completion checks. It preserves each write's `ts` and each read's `read_ts`.
Private writes remain in the Java request context and never reach Relive's
write endpoint. ReadGroup composition and snapshot ACK dependencies remain in
the replay client. No source transaction is re-executed.

The bridge encodes a key as lowercase hex of `00 ++ full opaque key`. This fixed
prefix preserves unsigned ordering, prefix keys and the empty key without
changing Relive's nonempty-key protocol. A fresh store is required for this
representation. Values use ordinary hex, with null reserved for a tombstone.
Relive's decoded limits restrict bridge keys to 65,535 bytes and values to
65,536 bytes; larger input fails explicitly. All batch members are validated
before sending any, then split at Relive's 10,000-row/8 MiB JSON limits. A later
chunk failure can leave earlier chunks applied; no automatic retry is made.

Reads use the streaming HTTP endpoint and require its header, ordered in-range
entries and matching terminal counts, followed by normal EOF. Missing completion
or malformed/trailing output fails the RPC even after partial values. The bridge
checks representation and completion; full value correctness still requires an
independent oracle. Neither the bridge nor ordinary replay proves a remote
server chose the right historical value merely from a matching count.

Persistent binding includes the Trace binding and value mode. Headerless callers
use a reserved ordinary-caller binding on first access; a populated ordinary
store cannot later accept a different Trace. Reconnecting to a Trace-bound store
requires its original binding. Bindings are guards, not authentication.
The bridge caches the accepted binding and description. Keep its HTTP target
attached to the same persisted store; changing storage requires restarting the
bridge and binding again. Live retargeting to a different index is unsupported.

The description reports `ACK_WAL` with `shared_nfs_fsync` durability. Each HTTP
write returns after shared-journal force, atomic publication and directory force
of the journal byte boundary, and visible application; bridge flush
does not force L1 upload or maintenance drain. Account for submitted writes, stop
new submissions, and measure backend maintenance separately using Relive's
[maintenance statistics](../index/relive/docs/local-protocol.md). Explicit full
compaction can rewrite stable L1 and overlap background work; its duration is
not a normal backlog-drain measurement. Record that extra work and retained-file
costs. Compare this ACK
with equivalent guarantees, or report it separately from memory ACKs. Actual
CFS durability and cross-host visibility require deployment acceptance.
The reported location includes the engine's CFS directory and object-storage
identity. HTTP exchanges have a 60-second absolute deadline, including stream
bodies; an overlong scan fails and is not silently truncated or retried.

The default tests use an HTTP boundary fixture. After packaging Relive, run the
explicit engine integration check with its test-only filesystem/object fixture:

```bash
mvn -f index/relive/pom.xml -Drelive.build.directory="$PWD/build/index/relive" package
mvn -Dtest=ReliveIntegrationCheck \
  -Drelive.test.jar="$PWD/build/index/relive/relive.jar" \
  -Drelive.test.classes="$PWD/build/index/relive/test-classes" test
```

This checks real engine/HTTP/gRPC integration with hand-derived historical
values. It is not CFS/COS acceptance, full-Trace validation or a performance run.

For explicit cloud acceptance, the same test class has a `main` entry point:
`populate HTTP_URL_A HTTP_URL_B` runs the oracle through two gRPC bridges to two
Relive processes sharing one fresh CFS directory and COS prefix. After stopping
both processes and reopening that state, `reopen HTTP_URL` checks retained
history, empty values, binding and private-write isolation. Run with the SIBench
JAR, compiled test classes, JUnit and Hamcrest on the classpath. Budget 300 seconds
for populate and 120 seconds for reopen, bound process shutdown separately, and
keep Java temporary files and all run evidence on cloud storage. This is a
single-host functional check; use the normal replay/verification workflow for
selected Traces and measure performance separately.

Use `populate-journal HTTP_URL_A HTTP_URL_B` on a separate fresh state to exercise
the same oracle without explicit flush/compaction. Give each engine at least a
256 KiB Memtable budget. The check requires zero flushed files, positive memory
and journal bytes, and no uploaded L1 bytes before it succeeds. Terminate both
engine processes without graceful shutdown, reopen with an empty process cache,
and run `reopen HTTP_URL` to test the acknowledged journal tail. Record Stats
before termination and after reopening. This process-loss check does not certify
host-power loss, storage-server failure or cross-host durability.

`ReliveLoadCheck HTTP_WRITER HTTP_OBSERVER CLIENTS FRESH_REPORT [MUTATIONS]` is a separate,
explicit cloud diagnostic on the same test classpath. It accepts 1 or 4 write
clients and 64–8,192 mutations in multiples of 64 (default 64), with half as many
snapshot-zero Gets. Versions `1..MUTATIONS` and their values are fixed before
submission; descending delivery retains deletion and reinsertion histories on
eight keys. The check observes another instance after all write ACKs, flushes,
waits for both instances' background maintenance to become idle, then checks
old, middle and latest snapshots. It does not force final compaction.
Use fresh engine state and explicit memory/process budgets. The workload has a
600-second shared deadline, each data RPC has a fresh 30-second deadline, and
normal drain has a 120-second bound. Budget priming, final verification and
setup/teardown separately. The reported
ACK-to-observer interval ends at one correct Scan's completion; it is an upper
bound on index visibility, not a lakehouse commit metric. This finite test does
not establish sustained throughput or full-Trace acceptance.
Drain timing starts after all foreground reads and writes finish. Separate
last-write-submission/last-write-ACK-to-observed-idle fields include any remaining
foreground reads and the polling interval; they do not timestamp the first
instant when background work became idle.
