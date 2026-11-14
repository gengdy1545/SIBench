# Trace contract

A Trace is an immutable baseline and workload for isolated index replay. Native `generate`, live `generate-pixels` and Trace-enabled [Pixels experiments](pixels-experiment.md) publish the same format. Workload configuration controls source execution; export preserves supported accesses and does not promise deterministic concurrent ordering.

## Selection and storage

`config/sibench.yaml` owns named benchmark entries and object-storage connections. Each entry references one source configuration and engine; YCSB entries also select one `workload` from that configuration. Scale and workload parameters remain in `config/benchmark/`. A sysbench entry without `workload` retains the source configuration's complete sequence of workloads.

```yaml
benchmarks:
  ycsb-a:
    config: config/benchmark/ycsb.yaml
    mode: api
    engine: rocksdb
    workload: workloada
    trace:
      generate: true
      directory: results/traces/ycsb-a
      upload:
        enabled: true
        path: cos://YOUR-BUCKET/sibench/traces/ycsb-a/
```

`trace.generate` controls only generation. Analysis and replay resolve the same named entry even when generation is disabled; they never regenerate or download implicitly. The local directory is persistent input, with no additional cache. Working databases, logs and effective configuration snapshots remain below the source configuration's `artifacts.directory`. Both working and persistent outputs must be fresh and nonoverlapping. Completed local bundles move to the persistent directory after normal native shutdown; failed moves retain the source bundle. Directory moves require the same filesystem; native and Pixels generation check this, resolved path overlaps and upload credentials before starting a database. Existing Traces are never overwritten.

`object_storage.aws` and `object_storage.cos` define endpoint, region, addressing (`path` or `virtual_hosted`) and credential environment-variable names. `s3://BUCKET/PREFIX` selects AWS; `cos://BUCKET/PREFIX` selects COS. Bucket and prefix belong only to the URI. Unknown schemes, traversal, ambiguous/encoded paths, queries and embedded credentials are rejected. The same resolver serves Index Service storage and reusable input transfers. Only connections actually opened read credentials; a local run needs none. A local S3-compatible service can be selected by setting the AWS connection's endpoint and addressing in an explicit test runtime config. Backend native clients keep their own I/O implementation and storage semantics.

An enabled upload requires `path`. Successful local generation uploads through the shared input-transfer implementation, verifies complete remote payload and inventory readbacks, and records the returned inventory identity in `trace-publication.json` beside the source run's effective configuration. A failed upload leaves the persistent local Trace usable, records the failure and fails the command. Content objects live at `PREFIX/objects/<sha256>`; the immutable selection inventory is published last at `PREFIX/manifests/<sha256>.json`. Uploads neither overwrite a mutable current pointer nor delete older data. Retire superseded inventories/payloads explicitly after checking their consumers.

The same transfer commands can publish a completed Trace or restore it on another host:

```bash
java -jar build/sibench/sibench.jar publish-input config/sibench.yaml \
  s3://YOUR-BUCKET/sibench/traces/ycsb-a/ results/traces/ycsb-a
java -jar build/sibench/sibench.jar fetch-input config/sibench.yaml \
  s3://YOUR-BUCKET/sibench/traces/ycsb-a/ INVENTORY_SHA256 results/traces/ycsb-a
java -jar build/sibench/sibench.jar check-replay config/sibench.yaml ycsb-a
```

Fetch requires a fresh local directory and verifies the inventory and payload identities before exposing its manifest. Use the inventory digest returned by publication, not the digest of the Trace's own `manifest.json`. Each analysis/replay result continues to bind the actual Trace identity; editing a configuration does not change an existing Trace. Each generation records its effective configuration and build identities separately from subsequent consumer settings.

## Native publication

| File | Contents |
| --- | --- |
| `baseline.pb.zst` | Initial index entries at version 0. |
| `workload.pb.zst` | Committed writes, successful snapshot reads and committed transaction boundaries. |
| `work-units.jsonl.zst` | Continuous work-unit IDs from 1 and their first source events. |
| `manifest.json` | Catalog, file inventories, counts, version coverage and source/build/configuration identities. |

Publication requires normal engine shutdown and validation. `INCOMPLETE` remains until atomic manifest publication; consumers reject incomplete or altered artifacts. Working databases are deleted afterward. Failures retain diagnostics and cannot resume; a later cleanup failure leaves the published Trace valid. Native rows and raw capture histories are not retained for replay.

Every catalog table must have baseline entries unless declared in `empty_baseline_tables`; declared empty tables must have none. Catalog identity binds all source index definitions, schema, benchmark and source commit, including for primary-only exports. Consumers need no source checkout. Source evidence binds expected per-table counts and completeness checks; it cannot independently certify an untrusted producer.

`trace.workload_requests` targets effective requests after projection: writes and individual read members, excluding baseline and transaction markers. Every selected workload receives its budget; admitted work drains and may exceed it. No-progress batches fail. Without a target, each configured stage runs once. `request-budget.json` records counts and overshoot.

YCSB loads once, closes the database and restores identical files for each independent workload. `traces.json` indexes the resulting named persistent Trace locations. The initial-state identity binds files, seed and baseline; a workload never inherits another workload's mutations. Native block caches are fresh; OS caches are not cleared.

### Pixels host

A remote host runs `pixels-host ADDRESS:PORT BUILD_DIRECTORY WORK_ROOT TOKEN_FILE`; configure `pixels_host: HOST:PORT` and supply `SIBENCH_PIXELS_HOST_TOKEN` to the client. PostgreSQL and the live sink stay on the generator. Local Pixels is the default.

The host authenticates requests, checks Java/native source identities and records actual host artifacts. Uploads have deadlines, reject escapes/overwrites and permit at most 4 GiB per shard. Sessions own fresh directories and child processes; only stopped, session-owned state can be deleted. Failed sessions retain diagnostics.

## Protobuf format

[index_trace.proto](../proto/index_trace.proto) defines `TraceRecord`: `transaction_begin`, `operation` or `transaction_commit`. Operations reuse [service requests](../proto/index_service.proto) for Get/Put/Delete/Scan, or describe a `ReadGroup`, with required source metadata. WriteBatch is assembled during replay and is not a Trace record. There is one supported format; clients and artifacts must match.

| Element | Encoding |
| --- | --- |
| Prefix | Big-endian int32 magic `0x53495042`. |
| Block header | Four big-endian int32s: message count, compressed size, uncompressed size, CRC32C of uncompressed bytes. |
| Payload | Independent level-3 Zstd frame of length-delimited TraceRecord messages. |
| Bounds | Target 1 MiB/block; at most 4096 messages; one delimited request at most 32 MiB. |
| Terminator | Four zero int32s; missing termination or trailing bytes fail. |

File descriptors bind path, physical SHA-256, logical SHA-256 of uncompressed delimited messages, bytes and counts. Baseline/workload identity hashes the logical digest's 32 bytes followed by the big-endian int64 semantic operation count. Semantic counts include read members and boundary markers; `effective_requests` excludes markers.

Manifest statistics distinguish primary/secondary entries, primitive operations, request types, markers, source projection, business write origins and uncompressed message bytes. Source counts can exceed exported counts. Statistics do not replace consumer validation of digests, accounting, positions and source coverage.

A read's `local_writes` is an ordered, unique-key list of scoped private Put/Delete entries with timestamp zero. Private Puts carry RowIDs; aligned `local_locators` carry alternate Locators, with empty entries for Delete or a zero Locator. Malformed encodings, missing requests/metadata, unknown fields and empty operations fail validation.

## Identity and versions

```text
PrimaryKey    = Table(u32 BE) ++ IndexID=0(u32 BE) ++ escaped(PK)
SecondaryKey  = Table(u32 BE) ++ IndexID(u32 BE) ++ escaped(IndexedValue) ++ escaped(PK)
RowID value   = row_id(i64 BE)
Locator value = file(i64 BE) ++ position(i64 BE)
```

`escaped` maps zero to `00 ff` and terminates a component with `00 00`, preserving unsigned byte order. Table IDs start at 1 in schema order; secondary IDs follow source index order. Nonunique secondary equality uses a prefix-range Scan; a complete secondary key uses Get.

Typed key components use `0x02` for NULL and `0x01` for non-NULL. Signed integers are sign-bit-flipped big-endian int64; timestamps use epoch microseconds with supported infinities at the extremes. Text is NUL-terminated UTF-8; embedded NUL is unsupported. PostgreSQL CHAR keys omit trailing blanks; DuckDB CHAR preserves them. Supported types are signed integers, timestamps and binary-ordered text. Arbitrary collations and composite/expression indexes are unsupported.

RowIDs are nonnegative, table-scoped int64s. SQL/RocksDB allocate from 1 and preserve identities on update. Pixels allocates from 0 and gives replacement rows new identities. Delete/reinsert always gets a new identity. Pixels Locator positions pack `(rgId << 32) | rgRowOffset`; storage GC is disabled during capture. Stored Puts carry RowIDs and optional Locator metadata; an omitted Locator is zero. Replay accepts only manifest-declared `value_kinds`: native sources provide `row_id`, Pixels also provides `locator`.

Baseline version is 0. Native transaction outcomes receive canonical resolution slots, including Abort and read-only outcomes. Export retains original committed versions, so gaps remain; `last_resolved_version` is the last exported committed version or zero. DuckDB uses native resolution order; PostgreSQL uses completion order, placing XID-bearing outcomes before read-only ties and then source chunk order. Pixels versions follow its timestamps assigned at source BEGIN in commit order. Wall time and replay completion do not define snapshots.

Reads select their actual native committed snapshot plus their own preceding visible writes. PostgreSQL uses each access's MVCC snapshot, command ID and subtransaction outcomes, including nested executors and savepoints. A read bound may be lowered across excluded Abort slots only after proving they contain no committed state; historical visibility is preserved.

## Work units and routing

Work units bind dispatch to source origins: statements for PostgreSQL/API, adopted accesses or row changes for DuckDB, applied transactions for Pixels. Statements with no index operation remain accounted for. Trace order follows transaction resolution and within-transaction order; source provenance can therefore differ from record order.

Baseline units group 4096 source rows across tables, keeping a row's expanded entries together. Controls use work-unit ID 0. Routing preserves recorded order and bindings; it does not reconstruct a server-side transaction or DAG.

## Operations and index expansion

Only committed writes enter shared replay state. Each commit retains the final action per physical key; intermediate writes are coalesced. Successful reads survive source rollback, with any private writes visible at the time. Those contexts may contain intermediate or subsequently aborted values and never modify shared history.

Positive matching Begin/Commit versions delimit committed source transactions. Nested, unmatched, mismatched or unclosed boundaries fail. Aborted boundaries/writes are omitted; successful reads from aborted transactions remain standalone requests. Boundaries are provenance and scheduling controls, not RPCs.

Source changes maintain the primary and every exported secondary index in IndexID order. A changed secondary key produces Delete(old) and Put(new); unchanged keys receive the new row version. Baseline expansion metadata validates complete row maintenance. Workload writes retain source event, work unit, mutation ordinal and business origin. The service must not expand them again.

`trace.secondary_indexes` defaults to false. Primary-only export omits secondary operations and whole read groups containing secondary members, while preserving committed boundaries and read visibility. The manifest declares `primary_only` or `all_source_definitions`. Replay cannot request absent indexes; full exports support either consumer selection. Pixels exports primary indexes only.

ReadGroup members share one table/read point and combine by value identity: intersection preserves the first member's order; union preserves first occurrence. Unknown combine modes fail. Within a table/snapshot, every index must identify a row with the same value and distinct visible rows must have distinct values. Each member is a separate RPC; group validation uses the combined expected count.

Scan orders unsigned keys, optionally reversed, and applies its limit after snapshot selection, private merging and deletion filtering. Get returns at most one entry. Private contexts permit at most 1,000,000 entries per request and 32 MiB. Committed/unknown-outcome transaction buffers, distinct-key write maps and aggregate pending private references each permit 1,000,000 items. PostgreSQL also bounds own-write/executor/subtransaction histories to 1,000,000 items each, decoded events to 16 MiB and compressed transaction spools to 4 GiB. Exceeding limits fails generation with diagnostics.

## Access projection

Captured reads describe complete native index output before SQL filtering, joins, sorting or aggregation. Sequential scans produce no synthetic index requests. Downstream LIMIT does not become a Search limit. Incomplete, ambiguous or unsupported access facts prevent publication.

DuckDB supports adopted single-column ART bounds and repeated point probes, deduplicated into union groups. Abandoned/internal-history attempts are diagnostics. Interrupted primary-key equality is accepted only when identical equality predicates and one emitted matching PK prove complete output. Other interrupted outputs must exhaust base and transaction-local matches.

YCSB Scan preserves its explicit maximum result count. Failed/partial SQL executors do not certify reads; completed nested reads can remain valid even if later source work rolls back.

## Replay preflight

`check-replay CONFIG BENCHMARK` validates without an adapter; ordinary replay performs the same preflight. It checks identities/digests, catalog, counts, version coverage, final-write uniqueness, provenance, snapshots/private contexts, routing, value capability and supply. Baseline validation is fused with loading and accepts only timestamp-zero Puts.

Required supply is warmup + `ceil(rate × measurement_seconds × (1 + margin))`, counted in effective requests. Insufficient supply fails before sending. Baseline and markers do not count; private entries add no requests. Warmup membership follows input order through completion. Finite supply does not establish achievable throughput, and replay has no segmented resume/checkpoint support.

Baseline loading drains before workload dispatch. Snapshot reads wait for required pending write ACKs; failures cancel outstanding work. [Service validation](index-service.md#results-and-failure-handling) checks counts and completion, with independent value/order acceptance performed separately.

## Source scope

Only clean pinned sources and supported schemas are accepted. Generation creates private databases; dynamic DDL and arbitrary external databases are unsupported. Baseline counts must match every table.

| Source | Scope and limits |
| --- | --- |
| YCSB | UTF-8 keys, binary fields, one primary index; independent official A–F workloads. F remains separate Read and Update calls. |
| sysbench | sbtest tables with id primary and k secondary indexes; configured supported scripts, including native conflict retries. |
| HyBench native | Eight tables, eight primary and thirteen secondary indexes; XP runs AT/IQ. Native transaction conflicts roll back and continue; other SQL errors fail. |
| HyBench CDC | Same CSV baseline in uninstrumented PostgreSQL and Pixels; TP/AT only. One Pixels transaction per committed source transaction. SQL reads, read-only/aborted transactions and secondary indexes are outside the exported scope. |

Native capture rejects primary-key assignments, including later-aborted attempts. Pixels rejects actual key changes but permits unchanged-key assignments. CDC requires FULL replica identity. Its index lookups must agree with each following change; write/commit, asynchronous sink or visibility failures prevent publication because Pixels cannot undo applied index entries.

PostgreSQL HyBench `query_coverage` binds IQ-1–IQ-6 source success/failure counts, executed indexes and exported accesses; main-IQ successes must match client histograms. Indexed-access coverage is not complete SQL/HTAP coverage. CDC scope is recorded separately in `source_evidence.workload_scope`.

Setup, execution and shutdown have separate bounds; cleanup preserves the first failure. Rewrite, physical version GC, unknown-outcome write retries and crash recovery are unsupported. [Pixels experiment inputs](pixels-experiment.md#prepare-once-restore-for-each-run) contain row changes in a separate format from Index Trace.
