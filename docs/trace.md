# Trace contract

A Trace is an immutable baseline plus workload for isolated index replay. Native `generate` and Trace-enabled [Pixels experiments](pixels-experiment.md) publish the same format. Workload configuration controls source execution; export keeps the supported accesses and does not promise a deterministic concurrent order.

## Selection and storage

`config/sibench.yaml` holds the named benchmark entries and the object-storage connections. Each entry references one source configuration and engine, which also fixes the generation mode (RocksDB records YCSB API calls; PostgreSQL and DuckDB capture SQL); scale and workload parameters stay in `config/benchmark/`. HyBench entries set `workload: tp` or `xp`, and YCSB entries pick one workload of their shared configuration. A sysbench workload is a named run of one script with its own zipfian exponent, so the public profile yields two Traces of `oltp_read_write` (`sysbench-zipf080`, `sysbench-zipf099`); each loads a fresh database. An entry names its workload unless the profile has only one.

```yaml
benchmarks:
  ycsb-a:
    config: config/benchmark/ycsb.yaml
    engine: rocksdb
    workload: workloada
    trace:
      generate: true
      directory: results/traces/ycsb-a
      upload:
        enabled: true
        path: cos://YOUR-BUCKET/sibench/traces/ycsb-a/
```

`trace.generate` only controls generation: analysis and replay resolve the same entry and never regenerate or download. The local `trace.directory` is the persistent input; there is no other cache. Working databases, logs and effective-configuration snapshots stay under the source configuration's `artifacts.directory`. Working and persistent outputs must be fresh and nonoverlapping, and existing Traces are never overwritten. A finished bundle moves to the persistent directory after normal shutdown (a failed move keeps the source bundle), which requires one filesystem. Native and Pixels generation check this, path overlaps and upload credentials before starting a database.

`object_storage.aws` and `object_storage.cos` define endpoint, region, addressing (`path` or `virtual_hosted`) and credential environment-variable names. `s3://BUCKET/PREFIX` selects AWS and `cos://BUCKET/PREFIX` selects COS; bucket and prefix belong to the URI only. Unknown schemes, traversal, ambiguous or encoded paths, queries and embedded credentials are rejected. The same resolver serves Index Service storage and input transfers. Credentials are read only for connections that are opened, so a local run needs none. A local S3-compatible service can be used by setting the AWS endpoint and addressing in an explicit test config. Backend native clients keep their own I/O and storage semantics.

An enabled upload needs `path`. After local generation the shared transfer implementation uploads the Trace, verifies full payload and inventory readbacks, and records the inventory identity in `trace-publication.json` beside the run's effective configuration. A failed upload leaves the local Trace usable, records the failure and fails the command. Content objects are `PREFIX/objects/<sha256>`; the immutable selection inventory is written last at `PREFIX/manifests/<sha256>.json`. Uploads never overwrite a current pointer or delete older data; retire superseded inventories and payloads explicitly after checking their consumers.

The same commands publish a completed Trace or restore it on another host. For this example, select the `ycsb-a` entry above in the runtime:

```bash
java -jar build/sibench/sibench.jar publish-input config/sibench.yaml \
  s3://YOUR-BUCKET/sibench/traces/ycsb-a/ results/traces/ycsb-a
java -jar build/sibench/sibench.jar fetch-input config/sibench.yaml \
  s3://YOUR-BUCKET/sibench/traces/ycsb-a/ INVENTORY_SHA256 results/traces/ycsb-a
java -jar build/sibench/sibench.jar check-replay config/sibench.yaml ycsb-a
```

Fetch needs a fresh local directory and verifies inventory and payload identities before exposing the manifest. Use the inventory digest returned by publication, not the digest of the Trace's `manifest.json`. Results bind the actual Trace identity, so editing a configuration never changes an existing Trace; each generation records its effective configuration and build identities separately from consumer settings.

## Native publication

| File | Contents |
| --- | --- |
| `baseline.pb.zst` | Initial index entries at version 0. |
| `workload.pb.zst` | Committed writes, successful snapshot reads and committed transaction boundaries. |
| `work-units.jsonl.zst` | Continuous work-unit IDs from 1 and their first source events. |
| `manifest.json` | Catalog, file inventories, counts, version coverage and source/build/configuration identities. |

Publication needs normal engine shutdown and validation. The Trace stays `INCOMPLETE` until the manifest is published atomically, and consumers reject incomplete or altered artifacts. Working databases are deleted afterwards. A failure keeps diagnostics and cannot resume; a later cleanup failure leaves the published Trace valid. Native rows and raw capture histories are not kept for replay.

Every catalog table needs baseline entries unless it is listed in `empty_baseline_tables`, in which case it must have none. The catalog identity binds all source index definitions, schema, benchmark and source commit, also for primary-only exports, so consumers need no source checkout. Source evidence binds expected per-table counts and completeness checks; it cannot certify an untrusted producer.

`trace.workload_requests` targets effective requests after projection: writes and individual read members, excluding baseline and transaction markers. Every selected workload gets its budget; admitted work drains and may exceed it, and a batch with no progress fails. Without a target each configured stage runs once. `request-budget.json` records counts and overshoot.

- **YCSB** loads once, closes the database and restores identical files for each independent workload. `traces.json` indexes the resulting Trace locations. The initial-state identity binds files, seed and baseline; no workload inherits another's mutations. Native block caches are fresh; OS caches are not cleared.
- **HyBench** publishes `hybench-tp` and `hybench-xp` independently from one source configuration. TP runs the upstream TP-1–TP-18 mix with `native.clients`, including read-only transactions and reads inside updates, so it is not write-only. XP runs AT-1–AT-6 and IQ-1–IQ-6 with explicit transaction and query client counts. Each selection loads a fresh native database under `artifacts.directory/tp` or `/xp`. Scale and schema match, but the unseeded upstream generator does not promise identical baseline contents. Validate replacements and switch consumers before deleting old payloads.

### Pixels host

A remote host runs `pixels-host ADDRESS:PORT BUILD_DIRECTORY WORK_ROOT TOKEN_FILE`. Configure `pixels_host: HOST:PORT` and give the client `SIBENCH_PIXELS_HOST_TOKEN`; the file-applying sink stays on the controller, and CDC preparation never contacts the host. Local Pixels is the default.

The host authenticates requests, checks Java/native source identities and records the actual host artifacts. Uploads have deadlines, reject escapes and overwrites, and are limited to 4 GiB per shard. Each session owns fresh directories and child processes; only stopped, session-owned state can be deleted. Failed sessions keep diagnostics.

## Protobuf format

[index_trace.proto](../proto/index_trace.proto) defines `TraceRecord`: `transaction_begin`, `operation` or `transaction_commit`. An operation reuses a [service request](../proto/index_service.proto) (Get/Put/Delete/Scan) or describes a `ReadGroup`, with required source metadata. WriteBatch is assembled during replay and is not a Trace record. There is one supported format; clients and artifacts must match.

| Element | Encoding |
| --- | --- |
| Prefix | Big-endian int32 magic `0x53495042`. |
| Block header | Four big-endian int32s: message count, compressed size, uncompressed size, CRC32C of the uncompressed bytes. |
| Payload | Independent level-3 Zstd frame of length-delimited TraceRecord messages. |
| Bounds | Target 1 MiB per block; at most 4096 messages; one delimited request at most 32 MiB. |
| Terminator | Four zero int32s; a missing terminator or trailing bytes fail. |

`proto_files.baseline` and `proto_files.workload` each describe one stream, stored as `baseline.pb.zst` and `workload.pb.zst`: physical SHA-256, logical SHA-256 of the uncompressed delimited messages, bytes, operation count and statistics. A baseline or workload identity hashes the 32-byte logical digest followed by the big-endian int64 semantic operation count. Semantic counts include read members and boundary markers; `effective_requests` excludes markers.

Manifest statistics separate primary and secondary entries, primitive operations, request types, markers, source projection, business write origins and uncompressed message bytes. Source counts can exceed exported counts. Statistics do not replace consumer validation of digests, accounting, positions and source coverage.

A read's `local_writes` is an ordered, unique-key list of scoped private Put/Delete entries with timestamp zero. Private Puts carry RowIDs; the aligned `local_locators` carry alternate Locators (empty for Delete, zero for none). Malformed encodings, missing requests or metadata, unknown fields and empty operations fail validation.

## Identity and versions

```text
PrimaryKey    = Table(u32 BE) ++ IndexID=0(u32 BE) ++ escaped(PK)
SecondaryKey  = Table(u32 BE) ++ IndexID(u32 BE) ++ escaped(IndexedValue) ++ escaped(PK)
RowID value   = row_id(i64 BE)
Locator value = file(i64 BE) ++ position(i64 BE)
```

`escaped` maps zero to `00 ff` and ends a component with `00 00`, which preserves unsigned byte order. Table IDs start at 1 in schema order; secondary IDs follow source index order. Nonunique secondary equality is a prefix-range Scan; a complete secondary key is a Get.

Typed key components start with `0x02` for NULL and `0x01` for non-NULL. Signed integers are sign-bit-flipped big-endian int64. Timestamps are epoch microseconds with supported infinities at the extremes. Text is NUL-terminated UTF-8 (embedded NUL is unsupported); PostgreSQL CHAR keys drop trailing blanks and DuckDB CHAR keeps them. Supported types are signed integers, timestamps and binary-ordered text. Arbitrary collations and composite or expression indexes are unsupported.

RowIDs are nonnegative, table-scoped int64s. SQL and RocksDB allocate from 1 and keep the identity on update; Pixels allocates from 0 and gives replacement rows new identities. Delete followed by reinsert always gets a new identity. Pixels Locator positions pack `(rgId << 32) | rgRowOffset`, and storage GC is disabled during capture. Stored Puts carry RowIDs and optional Locator metadata (omitted means zero). Replay accepts only the manifest's `value_kinds`: native sources provide `row_id`, Pixels also `locator`.

Baseline version is 0. Native transaction outcomes, including Abort and read-only ones, get canonical resolution slots. Export keeps the original committed versions, so gaps remain, and `last_resolved_version` is the last exported committed version or zero. DuckDB uses its native resolution order; PostgreSQL uses completion order, with XID-bearing outcomes before read-only ties and then source chunk order; Pixels versions follow the timestamps assigned at source BEGIN, in commit order. Wall time and replay completion never define snapshots.

A write uses its recorded committed version as `ts`; a read uses its recorded snapshot as `read_ts`, which is never replaced by the enclosing transaction's eventual commit version or a replay-time timestamp. Every backend returns the newest version at or below `read_ts` and then applies the request's private overlay, which holds the reader's own preceding visible writes. PostgreSQL uses each access's MVCC snapshot, command ID and subtransaction outcomes, including nested executors and savepoints. A read bound may be lowered across excluded Abort slots only after proving they hold no committed state.

## Work units and routing

Work units bind dispatch to source origins: statements for PostgreSQL and API sources, adopted accesses or row changes for DuckDB, applied transactions for Pixels. Statements without index operations stay accounted for. Trace order follows transaction resolution and the order within a transaction, so source provenance can differ from record order.

Baseline units group 4096 source rows across tables and keep a row's expanded entries together. Controls use work-unit ID 0. Routing preserves recorded order and bindings and does not rebuild a server-side transaction or DAG.

## Operations and index expansion

Only committed writes enter shared replay state, and each commit keeps the final action per physical key, so intermediate writes are coalesced. Successful reads survive source rollback with the private writes visible at the time. Those contexts may hold intermediate or later-aborted values and never modify shared history.

Matching positive Begin/Commit versions delimit committed source transactions; nested, unmatched, mismatched or unclosed boundaries fail. Aborted boundaries and writes are omitted, and successful reads from aborted transactions stay as standalone requests. Boundaries are provenance and scheduling controls, not RPCs.

A source change maintains the primary and every exported secondary index in IndexID order: a changed secondary key produces Delete(old) and Put(new), an unchanged one gets the new row version. Baseline expansion metadata validates complete row maintenance. Workload writes keep source event, work unit, mutation ordinal and business origin. The service must not expand them again.

`trace.secondary_indexes` defaults to false. A primary-only export omits secondary operations and whole read groups with secondary members, and keeps committed boundaries and read visibility. The manifest declares `primary_only` or `all_source_definitions`; replay cannot request absent indexes, and full exports support either consumer selection. Pixels exports primary indexes only.

ReadGroup members share one table and read point and combine by value identity: intersection keeps the first member's order, union keeps first occurrence, and unknown combine modes fail. Within a table and snapshot every index must identify a row with the same value, and distinct visible rows must have distinct values. Each member is a separate RPC, and group validation uses the combined expected count.

Scan orders unsigned keys, optionally reversed, and applies its limit after snapshot selection, private merging and deletion filtering. Get returns at most one entry.

Limits: a private context holds at most 1,000,000 entries and 32 MiB per request. Committed and unknown-outcome transaction buffers, distinct-key write maps and aggregate pending private references each allow 1,000,000 items. PostgreSQL also bounds own-write, executor and subtransaction histories to 1,000,000 items each, decoded events to 16 MiB and compressed transaction spools to 4 GiB. Exceeding a limit fails generation with diagnostics.

## Access projection

Captured reads describe the complete native index output before SQL filtering, joins, sorting or aggregation. Sequential scans produce no synthetic index requests, and a downstream LIMIT does not become a Search limit. Incomplete, ambiguous or unsupported access facts prevent publication.

DuckDB supports adopted single-column ART bounds and repeated point probes, which are deduplicated into union groups; abandoned or internal-history attempts are diagnostics only. An interrupted primary-key equality is accepted only with identical equality predicates and one emitted matching PK, which prove complete output; any other interrupted output must exhaust base and transaction-local matches.

YCSB Scan keeps its explicit maximum result count. Failed or partial SQL executors do not certify reads; completed nested reads stay valid even if later source work rolls back.

## Replay preflight

`check-replay CONFIG BENCHMARK` validates without an adapter, and ordinary replay runs the same preflight. It checks identities and digests, catalog, counts, version coverage, final-write uniqueness, provenance, snapshots and private contexts, routing, value capability and supply. Baseline validation is fused with loading and accepts only timestamp-zero Puts.

Required supply is warmup + `ceil(rate × measurement_seconds × (1 + margin))` effective requests; baseline, markers and private entries do not count. Too little supply fails before sending. Warmup membership follows input order through completion. Finite supply does not establish achievable throughput, and replay has no segmented resume or checkpoint.

The baseline drains before workload dispatch. Snapshot reads wait for the pending write ACKs they need, and a failure cancels outstanding work. [Service validation](index-service.md#results-and-failure-handling) checks counts and completion; independent value and order acceptance is separate.

An explicit workload-only diagnostic, after `mvn package`:

```bash
java -cp build/sibench/test-classes:build/sibench/sibench.jar \
  org.sibench.analysis.TraceProfileCheck TRACE_DIRECTORY FRESH_REPORT.json
```

It decodes the whole workload with the canonical decoder and checksum checks, reports source, write and read timestamp ranges and private-context counts, and aggregates full encoded keys into 4,096 hash bins. Bin collisions make this unusable as exact key hotness, unique-key counts or cache hit rates, and Scan bins describe start keys, not returned rows or I/O. It sends no service requests and does not replace preflight, baseline validation or source completeness evidence. Give it an explicit heap, time and storage budget.

## Source scope

Only clean pinned sources and supported schemas are accepted. Generation creates private databases; dynamic DDL and arbitrary external databases are unsupported. Baseline counts must match every table.

| Source | Scope and limits |
| --- | --- |
| YCSB | UTF-8 keys, binary fields, one primary index; independent official A–F workloads. F stays separate Read and Update calls. |
| sysbench | `sbtest` tables with `id` primary and `k` secondary indexes; configured supported scripts, including native conflict retries. Skewed ids are not scrambled, so each table's hottest rows are its smallest ids. |
| HyBench native | Eight tables, eight primary and thirteen secondary indexes; TP runs TP-1–TP-18, XP runs AT/IQ. Native transaction conflicts roll back and continue; other SQL errors fail. |
| HyBench CDC | Same CSV baseline in uninstrumented PostgreSQL and Pixels; TP/AT only, one Pixels transaction per committed source transaction. SQL reads, read-only or aborted transactions and secondary indexes are outside the exported scope. |
| Sysbench CDC | Baseline exported from the same PostgreSQL database that runs `oltp_read_write` with point and range SELECTs disabled. Pixels applies the actual committed changes, including DELETE/reinsert; UPDATEs affecting zero rows produce no row changes. The Trace covers Pixels primary-index calls, not source SQL reads or secondary indexes. |

Native capture rejects primary-key assignments, including those of later-aborted attempts. Pixels rejects actual key changes but allows unchanged-key assignments. CDC needs FULL replica identity, and its index lookups must agree with each following change. Write, commit, asynchronous sink or visibility failures prevent publication because Pixels cannot undo applied index entries.

PostgreSQL HyBench `query_coverage` binds IQ-1–IQ-6 source success and failure counts, executed indexes and exported accesses, and main-IQ successes must match the client histograms. Indexed-access coverage is not complete SQL/HTAP coverage. CDC scope is recorded separately in `source_evidence.workload_scope`.

Setup, execution and shutdown have separate bounds, and cleanup preserves the first failure. Rewrite, physical version GC, unknown-outcome write retries and crash recovery are unsupported. [Pixels experiment inputs](pixels-experiment.md#prepare-once-restore-for-each-run) hold row changes in a format separate from the Index Trace.
