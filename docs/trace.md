# Trace contract

The `api`, `capture` and `cdc` modes publish this single format from one explicitly selected native engine. The engine owns rows, transactions and snapshots. Exported Trace contains committed updates and committed-snapshot queries; aborted writes and transaction-local searches are excluded. Source support and user commands are listed in [README](../README.md).

## Native publication

A complete artifact contains:

| Path | Contents |
| --- | --- |
| `baseline.pb.zst` | Initial exported index entries at version 0. |
| `workload.pb.zst` | Committed index operations with source transaction boundaries/outcomes. |
| `work-units.jsonl.zst` | Operation origins (`work_unit_id`, first `source_event`) for coverage checks and client routing. |
| `manifest.json` | Catalog, source/configuration/build identities, file inventories, statistics and version coverage. |

Generation keeps `INCOMPLETE` until the engine closes normally and the manifest is atomically published. Consumers reject incomplete or altered artifacts. Failure, including a failed publication, retains diagnostics and the closed native database, stops later benchmarks and cannot resume; crash recovery and reusable initial-state databases are unsupported.

The native database is a working state of generation, not part of the artifact. Each engine keeps only what export needs: RowID identities (a baseline mapping and one identity per inserted row), commit-to-version mapping and the transient capture facts of the current window. The SQL engines retain no earlier row versions; RocksDB keeps timestamped row versions for its native snapshots until close. No engine retains read points or raw capture logs, and the data directory is deleted only after the Trace is published. In `cdc` mode the source PostgreSQL database and the Pixels run directory (etcd, metadata, Retina files and index) are such working state. A cleanup failure after publication leaves a valid Trace and reports the remaining directory. A Trace therefore supports index replay, not reading native row contents.

`source_evidence` binds effective configuration, client/engine identities, expected per-table baseline counts and source-completeness evidence. Source facts are checked while they are projected and then discarded; YCSB exports API operations directly. These identities do not independently certify an untrusted producer's completeness.

`catalog_sha256` binds the complete table/index definitions, benchmark, source commit, schema digests and source schema configuration, even for primary-only export. Consumers validate the embedded catalog without opening the source checkout. The source directory is informational. `value_kinds` declares the value kinds a Trace carries: SQL and RocksDB generation declare `row_id`; Pixels CDC declares `row_id` and `locator`. A Pixels Locator is the row version's location when Retina wrote it: `file` is the Pixels file id and `position` is `(rgId << 32) | rgRowOffset`. Storage GC, which would move rows, is disabled during generation. Replay rejects a value kind the Trace does not declare before creating/opening a backend.

A RowID is a non-negative int64 identity scoped to its table. SQL and RocksDB generation allocate from 1 and preserve the identity across updates. Pixels allocates from 0 per table and assigns a new RowID to each replacement row on update. Deleting and reinserting a key also assigns a new RowID. Consumers must use the selected index version's value, not assume a RowID is stable across updates in every engine.

`trace.workload_requests` counts exported writes and SearchGroup members (or standalone Get/Scan) after committed-state coalescing and index filtering. Baseline entries and transaction markers are excluded. Generation repeats finite source batches on the same database, distributes the remaining budget across remaining workloads, and drains admitted work. HyBench native capture runs AP once before allocating the remainder to TP/XP; live Pixels CDC shares the budget between TP and AT with AP/IQ disabled. Already admitted work may exceed the target, especially a whole DuckDB window or PostgreSQL backlog. No-progress batches fail. Omitting the target runs each configured batch once; `request-budget.json` records actual counts and overshoot.

`trace.secondary_indexes` defaults to false, exporting only primary-index requests while retaining source secondary indexes. Native capture can export all source indexes; Pixels CDC rejects secondary-index export. Replay's separate `target.secondary_indexes.enabled` setting cannot request indexes absent from the Trace. [Index expansion and filtering](#operations-and-index-expansion) preserve complete query groups and transaction outcomes.

### Pixels host

Live Pixels generation loads the same bound baseline independently into source PostgreSQL and Pixels, and starts workloads after both loads pass their per-table checks. `load.rows_per_file` controls the converted LOAD shard size and `load.threads` controls file consumers. The run retains its effective schema, load reports and capture progress.

The `pixels-host` agent authenticates requests with a token and verifies the client's Java build and native source/capacity identity before loading. It records the host's actual build in the result. Requests have upload/response deadlines; each uploaded shard is limited to 4 GiB. Uploads reject path escapes and overwrites. Failed uploads remove their partial file and terminate the session. Logs redact tracer tokens.

Each session owns a fresh working directory and its child processes. Failed sessions retain diagnostics. Deletion requires a directory created and stopped by that session; an existing directory cannot be adopted. The controller stops the host's services before Trace publication and working-state deletion. A cleanup failure after publication identifies the remaining directory without invalidating the Trace.

## Protobuf format

[index_trace.proto](../proto/index_trace.proto) contains operation envelopes and source positions. It imports [index_service.proto](../proto/index_service.proto), which defines both the RPCs and the Get, Put, Delete and Scan request types reused by Trace. A Get carries only key/read timestamp. A Scan carries its half-open byte bounds, read timestamp, direction and maximum result count. No bound/flag pattern is used to infer the operation at replay.

A SearchGroup contains Get/Scan members with one shared source position and expected count. TraceBegin and TraceEnd contain one source timestamp, with no separate transaction ID. They are source metadata, not RPCs. TraceEnd also records source mutation count and exported operation/write counts. Controls have no operation position. Replay assigns dispatch sequences internally; neither requests nor response chunks carry an application request ID. BatchWrite is built during baseline replay and is rejected in persisted Trace records.

The format has no legacy reader. Historical artifacts remain evidence for their matching frozen executable; they are not inputs for the current executable.

| Element | Encoding |
| --- | --- |
| File prefix | Big-endian int32 magic `0x53495042`. |
| Block header | Four big-endian int32 fields: message count, compressed bytes, uncompressed bytes, CRC32C of uncompressed bytes. |
| Block payload | Independent Zstd frame at level 3 containing length-delimited TraceRecord messages. |
| Limits | Target 1 MiB/block, at most 4096 messages; one request including its delimiter is at most 32 MiB. |
| Terminator | Four zero int32 fields; missing termination or trailing bytes are errors. |

Each `proto_files.baseline` / `proto_files.workload` entry records `path`, physical `sha256`, `logical_sha256` of the length-delimited uncompressed messages, semantic operation count, compressed `bytes` and the statistics below. Semantic operations count group members and source boundary markers separately. Baseline/workload identity hashes the canonical file's 32-byte logical digest followed by its big-endian int64 operation count.

| Statistic | Unit |
| --- | --- |
| `index_entries` | Writes and search members, excluding transaction markers. |
| `effective_requests` | Each write and each SearchGroup member (or standalone Get/Scan) once, excluding markers. |
| `primary_entries`, `secondary_entries` | Exported entries by index scope, including search members. |
| `request_types` | `put`, `delete`, `get`, `scan`, `union_query`, `intersection_query`; absent kinds count as zero. |
| `transaction_markers` | Source Begin, Commit and Abort outcomes separately; none is sent to the service. |
| `source_projection` | Excluded aborted writes/local search members, coalesced writes and peak buffered source operations. Write counts cover exported index entries; buffered operations exclude transaction-local searches, which are validated and dropped on arrival. |
| `write_origins` | Write entries with business-insert/update flags, not distinct rows. |
| `uncompressed_message_bytes` | Length-delimited protobuf bytes, excluding headers. |

Only exported operations count. A committed-snapshot search is retained even when its source transaction later aborts; its original read point remains valid. Native manifest transaction, local-search and logical-row-change totals describe source activity and may exceed exported totals. These units differ from returned rows and transport batches. Producer statistics do not replace consumer validation of file/logical identities, record counts, positions, transaction accounting and source-member coverage.

## Identity and versions

```text
PrimaryKey   = Table(u32 BE) ++ IndexID=0(u32 BE) ++ escaped(PK)
SecondaryKey = Table(u32 BE) ++ IndexID(u32 BE) ++ escaped(IndexedValue) ++ escaped(PK)
RowID value  = row_id(i64 BE)
Locator value = file(i64 BE) ++ position(i64 BE)
```

`escaped` maps each zero byte to `00 ff` and ends a component with `00 00`. It preserves unsigned byte order and makes component boundaries unambiguous independently of the source type. A stored Put carries RowID bytes; an optional 16-byte `TracePosition.locator` retains the alternative Locator for sources declaring that capability. An omitted locator decodes to zero file/position. The replay selects the declared value mode before sending Put; services never parse either representation.

TableIDs start at 1 in source-schema order; secondary IndexIDs follow each table's source index order. Nonunique secondary predicates search all matching primary keys.

`IndexKey` components are self-delimiting and preserve unsigned byte order. NULL is `0x02`; non-NULL values start with `0x01`. Signed integers use sign-bit-flipped big-endian int64; timestamps use microseconds from 1970 with supported infinities at the extremes. Text uses UTF-8 terminated by `0x00`; embedded NUL is unsupported. DuckDB CHAR preserves trailing spaces; PostgreSQL CHAR(n) compares without them, so its CHAR keys omit trailing blanks. Supported families are signed integers, timestamps and binary-ordered text; arbitrary collations and composite/expression indexes are unsupported.

The caller converts component bounds to half-open complete-key intervals. Equality on a nonunique secondary value is `[namespace ++ escaped(value), prefixSuccessor(namespace ++ escaped(value)))`; a known PK completes the key for Get. The backend separately escapes the entire opaque key and appends the descending timestamp, as defined in the [service contract](index-service.md#local-rocksdb-fixture).

Baseline version is 0. Native transaction outcomes receive contiguous canonical resolution slots, including Commit, Abort and read-only transactions. The source group timestamp is the resolution version; no second transaction identifier is serialized. DuckDB follows its native resolution order. PostgreSQL orders by native completion counter, places XID-bearing outcomes before read-only ties, and uses source chunk order for remaining ties. These read-only ties add no committed writes; slots are logical time, not physical completion timestamps. In `cdc` mode the slots follow Pixels transaction timestamps, which pixels-sink assigns at each source transaction's BEGIN in source commit order; only committed source transactions that changed rows reach Pixels, so there are no Abort or read-only slots. Native start IDs, wall time, log arrival and replay completion do not define visibility. The actual snapshot determines the committed read bound. At a read point, select the latest exported committed version at or below it; a selected tombstone means absent and cannot expose an older value.

Source operation order and `mutationOrdinal` identify original changes. Generation validates complete source transactions before filtering, then retains only the last action for each `(table, index, indexed_value, PK)` at that commit version. Intermediate values do not become separate same-version writes. A secondary key introduced and removed within one transaction may retain an absent-key tombstone; this is harmless and prevents older versions from resurfacing. Original source positions may therefore have gaps.

Snapshot searches keep the actual committed read bound. Transaction-local searches, including an entire group with any local member, are excluded. Source Abort slots remain as metadata with no exported writes. See [service visibility rules](index-service.md#data-interfaces). Native capture rejects primary-key assignments, including permutations and later-aborted attempts. Pixels CDC rejects actual key changes; assignments of the same key value are allowed.

## Work units and routing

`work-units.jsonl.zst` stores `work_unit_id` and its first `source_event`, with work units numbered continuously from 1; its SHA-256 is in the manifest.

DuckDB capture origins are adopted ART accesses or successful row-change facts; API origins are statement starts; Pixels CDC uses one work unit per applied source transaction. They follow increasing raw `source_event` order. PostgreSQL uses statements as work units and assigns `source_event` during its ordered transaction merge, preserving backend/request identities. Statements without index operations remain accounted for, never as synthetic accesses. Trace itself follows transaction resolution and within-transaction order.

Baseline work units group 4096 source rows across tables before index expansion; all entries for a row stay together. Controls use workUnitId 0; workload origins use positive IDs. Routing preserves recorded order and bindings; work units do not reconstruct a serverless DAG.

## Operations and index expansion

| Operation | Meaning |
| --- | --- |
| Baseline Put | One exported index entry at timestamp 0. |
| Get / Scan / SearchGroup | Explicit data request and read point, with native expected count; groups preserve one source access. |
| Put | The final committed value for one complete key at a source timestamp. |
| Delete | Versioned invalidation without a value payload. |
| Begin | Source transaction boundary, not an RPC. |
| TraceEnd | Source Commit/Abort outcome with exact exported operation/write counts; not an RPC. |

Source changes expand to the primary and every exported secondary index in increasing IndexID order. Changed secondary values delete the old identity and upsert the new; unchanged secondary keys also receive the new row version. Generation checks complete source expansions before coalescing. The service must not expand entries again.

Baseline members share source identity and RowID, with consecutive `baseline_position` / `baseline_size` metadata for complete index-maintenance validation. Workload writes retain source event, work unit, mutation ordinal and business write origin, but no expansion fields or deleted RowID. Generation validates the original source maintenance unit before coalescing/export. Query bounds, direction, group mode and member positions are derived from their request structure, never repeated as flags. `expected_count` belongs only to a query or query group; source mutation count belongs to TraceEnd. The capture pipeline's typed `Operation` view remains internal and is reconstructed for source validation, routing and analysis; it is not a second wire format.

`trace.secondary_indexes` defaults to false. The manifest declares `secondary_index_coverage` as `primary_only` or `all_source_definitions`; source database indexes remain intact. Primary-only export omits secondary writes/searches and whole groups containing secondary members. It preserves source outcomes, including empty transactions, read points and original provenance; exported request/write counts are recomputed.

Searches before a source transaction's first write cannot depend on its outcome and are exported at once, so long read-only transactions are not buffered. PostgreSQL capture likewise releases a long transaction's statements before its first write in bounded pieces: they become standalone snapshot searches ordered after every commit they observed, and the transaction keeps one resolution slot at its outcome. From the first write, the writer buffers the transaction, bounded by 1,000,000 source operations. It validates source accounting, expansions, groups and read points before excluding operations. Preflight validates exported counts, provenance, unique physical keys per commit version and snapshot-only searches before consumption-side filtering. Secondary-enabled consumption rejects a primary-only Trace before backend access; full exports support either setting. Baseline loading drains before workload sending. Snapshot queries wait for required pending write ACKs in replay; no synthetic request or server-side transaction control is added. Failures stop/cancel outstanding work.

Scan uses unsigned canonical key order, ascending unless reversed; a positive limit counts visible keys after version selection and deletion filtering. Get returns at most one visible entry.

## Access projection

Source capture represents a finally adopted index access's snapshot-visible output, including prior writes in its transaction. The committed Trace export subsequently excludes transaction-local accesses. It excludes subsequent non-index filtering, sorting, aggregation and other SQL operators, so SQL result counts may differ. For DuckDB, this is an adopted ART access; abandoned attempts and physical candidate order/cost remain diagnostics.

Complete index output remains a complete Search even if downstream JOIN/LIMIT discards it. All base and transaction-local matches must be exhausted. Incomplete output is rejected: no exact partial-range/order projection is implemented, and SQL LIMIT cannot become a Search limit.

| DuckDB access | Projection |
| --- | --- |
| Single-column ART equality/comparison | Actual typed bounds; intersect two bounds where present. |
| Supported repeated point probes | Deduplicate points; union group with the native visible result count. |
| Base and transaction-local probes | Matching conditions and the actual snapshot plus earlier local writes. |
| Interrupted primary-key equality | Accept only identical equality predicates and exactly one emitted matching PK; uniqueness proves complete logical output without a final empty fetch. |
| Abandoned/internal-history access or a statement without index access | No synthetic benchmark Search. |
| Missing identity/batch/close facts, unsupported conditions/types or ambiguous intra-statement ordering | Reject generation. |

YCSB API Scan retains its explicit maximum result count and may exhaust the range earlier. It does not alter capture early-stop rules. Failed SQL internals immediately undone without later-visible results stay diagnostic, not logical writes; unknown physical success is neither all-success nor zero work. Source validation retains earlier successful writes and accurately projected searches with the actual outcome before committed export filtering. An observed read of those internal temporary writes requires explicit supported semantics before projection.

## Replay preflight

`check-replay CONFIG TRACE` validates without invoking an adapter; replay already performs preflight. Checks cover manifest/catalog integrity, physical/logical digests, counts, contiguous resolution slots and the declared watermark, baseline expansions, final-write uniqueness/source bindings, client coverage, requested value capability and sufficient supply. Baseline validation is fused with loading and accepts only timestamp-zero Puts. Source failures, missing completion facts, unresolved transactions or snapshot mismatches prevent publication even if final rows appear correct.

Supply counts effective requests after filtering, excluding baseline/source markers, aborted writes and transaction-local searches; each SearchGroup member counts once. Required supply is warmup operations + ceil(rate × measurement seconds × (1 + margin)). Insufficient supply fails before loading/sending. Dispatch warmup, measurement windows and operation rates use this same primitive-operation count. Source terminal markers have no service call or latency samples. Warmup responses remain excluded from measured latency and completion rates even when they arrive after the measured window starts. Supply does not establish achievable throughput; Trace is finite and has no segmented resume/checkpoints.

Reports distinguish baseline/workload records, effective requests, transactions/outcomes, responses, in-flight work and backpressure. `last_resolved_version` includes Abort. RowID replay does not rewrite native rows. [Service checks](index-service.md#results-and-failure-handling) validate counts/completion and harness target binding, not every result identity; small independent semantic counterexamples are separate from ordinary replay. Native row/history auditing is outside the delivered workflow.

## Source scope

Only clean pinned benchmark sources with supported native schemas/configurations are accepted. Generation creates a private database; dynamic DDL and arbitrary external databases are unsupported. Baseline counts must match every configured table independently.

| Source | Data and behavior |
| --- | --- |
| YCSB / api | UTF-8 keys, configured binary fields, one primary index; real A/B/C/F/D/E calls. Baseline-only buffering requires successful worker completion and matching committed/emitted insert counts; workload calls are not batched. F remains separate Read and Update calls. |
| sysbench / capture | Configured sbtest tables, id primary and k secondary indexes; six configured OLTP scripts including delete/reinsert and native conflict retry. |
| HyBench / capture | Eight tables, eight primary and thirteen secondary indexes; AP → TP → concurrent XP. Native transaction conflicts (DuckDB write conflicts; PostgreSQL deadlocks and serialization failures) roll back that transaction and continue, retaining source diagnostics/outcomes; other SQL errors fail. |
| HyBench / cdc | The same upstream CSVs independently load an uninstrumented source PostgreSQL and Pixels. Pixels inputs are converted to `\|`-delimited text shards: unquoted empty CSV fields become NULL (`\N`), quoted empty strings stay empty, CHAR padding and ISO date conversion match PostgreSQL, and values requiring Pixels LOAD escaping are rejected. Text widths are derived from those inputs. PostgreSQL keeps its single baseline transaction; both loads and per-table counts must complete before CDC starts. A failed source transaction or Pixels visibility publication never yields a published Trace. pixels-sink applies the logical decoding stream (`REPLICA IDENTITY FULL`, no primary-key changes) to Pixels Retina, one Pixels transaction per source transaction. The Trace holds the primary-index calls of Retina: inserts as upserts, the key lookups of updates and deletes as snapshot point searches at the previous version, updates as upserts and deletes as logical deletes. SQL reads, aborted and read-only source transactions and secondary indexes are outside this scope; `trace.secondary_indexes` must be false. Each workload window ends with a marker transaction; when Pixels has applied it, every earlier source commit has been applied and projected. Marker transactions are not exported. A Pixels write or commit failure stops generation, because Pixels does not undo index entries on rollback. |

Pixels CDC runs a HyBench-derived TP/AT transaction workload: AP is skipped and XP has only AT clients. TP/AT SQL, including business reads, transaction boundaries and rollback behavior, is retained. SQL executes on PostgreSQL; Pixels applies CDC without AP/IQ SQL execution. Each batch's `resolved.props` records `apclient=0` and `xapclient=0`; the shared profile's `ap_clients` and `xp_query_clients` apply only to native capture. The `xp` stage uses `xp_transaction_clients` and the XP duration. The manifest's `source_evidence.workload_scope` records this subset; this is not full HyBench HTAP.

The patched pixels-sink ProtoWriter is a separate row-CDC writer, not the Index Trace format. It buffers complete transactions in source BEGIN order, restores each transaction's global row order across parallel table decoding, and checks exact transaction/table counts and contiguous global/table row positions before writing. Storage failures remain fatal to subsequent writes and close. The [CDC preparation command](cdc-input.md) uses a local closed-file inventory and publishes its input manifest after normal source shutdown and file validation. The upstream registry-backed writer can have already-completed rotated files, so registry registration alone never certifies a complete reusable input. File-driven Pixels experiments are not yet exposed.

Source retries require matching outcomes; the generator does not invent accesses by rerunning SQL or silently retry failed SQL. Setup, execution and shutdown have separate bounds; cleanup errors cannot replace the first failure. Successful publication requires normal shutdown, not crash recovery.
