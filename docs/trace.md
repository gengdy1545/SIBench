# Trace contract

Both `api` and `capture` modes publish this single format from one explicitly selected native engine. The engine owns rows, transactions and snapshots. Exported Trace contains committed updates and committed-snapshot queries; aborted writes and transaction-local searches are excluded. Source support and user commands are listed in [README](../README.md).

## Native publication

A complete artifact contains:

| Path | Contents |
| --- | --- |
| `baseline.pb.zst` | Initial exported index entries at version 0. |
| `workload.pb.zst` | Committed index operations with source transaction boundaries/outcomes. |
| `work-units.jsonl.zst` | Operation origins (`work_unit_id`, first `source_event`) for coverage checks and client routing. |
| `manifest.json` | Catalog, source/configuration/build identities, file inventories, statistics and version coverage. |

Generation keeps `INCOMPLETE` until the engine closes normally and the manifest is atomically published. Consumers reject incomplete or altered artifacts. Failure retains diagnostics, stops later benchmarks and cannot resume; crash recovery and reusable initial-state databases are unsupported.

The native database is a working state of generation, not part of the artifact. Each engine keeps only what export needs: RowID identities (a baseline mapping and one identity per inserted row), commit-to-version mapping and the transient capture facts of the current window. It retains no earlier row versions, read points or raw capture logs, and its data directory is deleted after normal shutdown. A Trace therefore supports index replay, not reading native row contents.

`source_evidence` binds effective configuration, client/engine identities, expected per-table baseline counts and source-completeness evidence. Source facts are checked while they are projected and then discarded; YCSB exports API operations directly. These identities do not independently certify an untrusted producer's completeness.

`catalog_sha256` binds the complete table/index definitions, benchmark, source commit, schema digests and source schema configuration, even for primary-only export. Consumers validate the embedded catalog without opening the source checkout. The source directory is informational. `value_kinds` currently declares only `row_id`; native replay rejects `locator` before creating/opening a backend.

Updates retain their RowID; a deleted and reinserted key receives a new one, so an old RowID never names a replacement row.

## Protobuf format

Requests use [index_service.proto](../proto/index_service.proto), wrapped with source positions by [index_trace.proto](../proto/index_trace.proto). SearchGroup is one request with one position per member; TraceBegin and TraceEnd are source metadata, not RPCs. The `request_types` counts describe serialized requests/groups; `effective_requests` counts their basic operations. Replay assigns request IDs and routing, and recomputes selected counts after filtering/batching.

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
| `effective_requests` | Each write and each SearchGroup member (or standalone Search) once, excluding markers. |
| `primary_entries`, `secondary_entries` | Exported entries by index scope, including search members. |
| `request_types` | `insert`, `upsert`, `logical_delete`, `relocate`, `point_search`, `range_search`, `union_search`, `intersection_search`; absent kinds count as zero. |
| `transaction_markers` | Source Begin, Commit and Abort outcomes separately; none is sent to the service. |
| `source_projection` | Excluded aborted writes/local search members, coalesced writes and peak buffered source operations. |
| `write_origins` | Write entries with business-insert/update flags, not distinct rows. |
| `uncompressed_message_bytes` | Length-delimited protobuf bytes, excluding headers. |

Only exported operations count. A committed-snapshot search is retained even when its source transaction later aborts; its original read point remains valid. Native manifest transaction, local-search and logical-row-change totals describe source activity and may exceed exported totals. These units differ from returned rows and transport batches. Producer statistics do not replace consumer validation of file/logical identities, record counts, positions, transaction accounting and source-member coverage.

## Identity and versions

```text
PrimaryKey   = (Table, IndexID=0, PK, VersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, VersionDescending)
LogicalValue = RowID
```

TableIDs start at 1 in source-schema order; secondary IndexIDs follow each table's source index order. Nonunique secondary predicates search all matching primary keys.

`IndexKey` components are self-delimiting and preserve unsigned byte order. NULL is `0x02`; non-NULL values start with `0x01`. Signed integers use sign-bit-flipped big-endian int64; timestamps use microseconds from 1970 with supported infinities at the extremes. Text uses UTF-8 terminated by `0x00`; embedded NUL is unsupported. DuckDB CHAR preserves trailing spaces; PostgreSQL CHAR(n) compares without them, so its CHAR keys omit trailing blanks. Supported families are signed integers, timestamps and binary-ordered text; arbitrary collations and composite/expression indexes are unsupported.

Backend keys are `Table(uint32 BE) ++ IndexID(uint32 BE) ++ [IndexedValue] ++ PK ++ (~Version int64 BE)`. Component-prefix equality is `[value, prefixSuccessor(value))`; inclusive/exclusive bounds adjust that endpoint.

Baseline version is 0. Native transaction outcomes receive canonical resolution slots in actual outcome order, including Commit, Abort and read-only transactions; `transactionId` is the resolution version. Native start IDs, wall time, log arrival and replay completion do not define visibility. The actual snapshot determines the committed read bound. At a read point, select the latest exported committed version at or below it; a selected tombstone means absent and cannot expose an older value.

Source operation order and `mutationOrdinal` identify original changes. Generation validates complete source transactions before filtering, then retains only the last action for each `(table, index, indexed_value, PK)` at that commit version. Intermediate values do not become separate same-version writes. A secondary key introduced and removed within one transaction may retain an absent-key tombstone; this is harmless and prevents older versions from resurfacing. Original source positions may therefore have gaps.

Snapshot searches keep the actual committed read bound. Transaction-local searches, including an entire group with any local member, are excluded. Source Abort slots remain as metadata with no exported writes. See [service visibility rules](index-service.md#data-and-snapshot-reads). Primary-key assignments, including permutations and later-aborted attempts, are unsupported.

## Work units and routing

`work-units.jsonl.zst` stores `work_unit_id`, `connection_id`, `query_id` and `source_event`, with work units numbered continuously from 1; its SHA-256 is in the manifest.

DuckDB capture origins are adopted ART accesses or successful row-change facts; API origins are statement starts. They follow increasing raw `source_event` order. PostgreSQL uses statements as work units and assigns `source_event` during its ordered transaction merge, preserving backend/request identities. Statements without index operations remain accounted for, never as synthetic accesses. Trace itself follows transaction resolution and within-transaction order.

Baseline work units group 4096 source rows across tables before index expansion; all entries for a row stay together. Controls use workUnitId 0; workload origins use positive IDs. Routing preserves recorded order and bindings; work units do not reconstruct a serverless DAG.

## Operations and index expansion

| Operation | Meaning |
| --- | --- |
| Baseline Insert | One exported index entry at version 0. |
| Search / SearchGroup | Recorded bounds/read point, with native expected count; groups preserve one source access. |
| Upsert | The final committed action for one physical index identity at a source version. |
| Logical delete | Versioned invalidation without a sentinel RowID/Locator. |
| Begin | Source transaction boundary, not an RPC. |
| TraceEnd | Source Commit/Abort outcome with exact exported operation/write counts; not an RPC. |

Source changes expand to the primary and every exported secondary index in increasing IndexID order. Changed secondary values delete the old identity and upsert the new; unchanged secondary keys also receive the new row version. Generation checks complete source expansions before coalescing. The service must not expand entries again.

Baseline expansion members share source identity, RowID and a complete consecutive `expansionPosition` / `expansionSize`. Exported workload writes are independent final entries with expansion `(0, 1)` and retain their original mutation ordinal, work unit and source event. Deletes carry `deleted_row_id`. SearchGroup has one source position per member and a shared group expectation. `TraceEnd.operations` counts exported writes and search members, and `TraceEnd.writes` counts exported writes. Its position's `expected_count` retains the source mutation count as a provenance bound.

`trace.secondary_indexes` defaults to false. The manifest declares `secondary_index_coverage` as `primary_only` or `all_source_definitions`; source database indexes remain intact. Primary-only export omits secondary writes/searches and whole groups containing secondary members. It preserves source outcomes, including empty transactions, read points and original provenance; exported request/write counts are recomputed.

Searches before a source transaction's first write cannot depend on its outcome and are exported at once, so long read-only transactions are not buffered. PostgreSQL capture likewise releases a long transaction's statements before its first write in bounded pieces: they become standalone snapshot searches ordered after every commit they observed, and the transaction keeps one resolution slot at its outcome. From the first write, the writer buffers the transaction, bounded by 1,000,000 source operations. It validates source accounting, expansions, groups and read points before excluding operations. Preflight validates exported counts, provenance, unique physical keys per commit version and snapshot-only searches before consumption-side filtering. Secondary-enabled consumption rejects a primary-only Trace before backend access; full exports support either setting. Baseline loading drains before workload sending. Snapshot queries wait for required pending write ACKs in replay; no synthetic request or server-side transaction control is added. Failures stop/cancel outstanding work.

Search uses canonical key order, ascending unless requested otherwise; a positive API limit is a maximum. The protocol's Locator/Relocate capability does not imply native relocation/GC support.

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

`check-replay CONFIG TRACE` validates without invoking an adapter; replay already performs preflight. Checks cover manifest/catalog integrity, physical/logical digests, counts, contiguous resolution slots and the declared watermark, baseline expansions, final-write uniqueness/source bindings, client coverage, requested value capability and sufficient supply. Baseline validation is fused with loading and accepts only version-zero Inserts. Source failures, missing completion facts, unresolved transactions or snapshot mismatches prevent publication even if final rows appear correct.

Supply counts effective requests after filtering, excluding baseline/source markers, aborted writes and transaction-local searches; each SearchGroup member counts once. Required supply is warmup operations + ceil(rate × measurement seconds × (1 + margin)). Insufficient supply fails before loading/sending. Dispatch warmup, measurement windows and operation rates use this same primitive-operation count. Source terminal markers have no service call or latency samples. Warmup responses remain excluded from measured latency and completion rates even when they arrive after the measured window starts. Supply does not establish achievable throughput; Trace is finite and has no segmented resume/checkpoints.

Reports distinguish baseline/workload records, effective requests, transactions/outcomes, responses, in-flight work and backpressure. `last_resolved_version` includes Abort. RowID replay does not rewrite native rows. [Service checks](index-service.md#results-and-validation) validate counts/completion and Flush completion/binding, not every result identity; independent full-data auditing is a separate explicit workflow.

## Source scope

Only clean pinned benchmark sources with supported native schemas/configurations are accepted. Generation creates a private database; dynamic DDL and arbitrary external databases are unsupported. Baseline counts must match every configured table independently.

| Source | Data and behavior |
| --- | --- |
| YCSB / api | UTF-8 keys, configured binary fields, one primary index; real A/B/C/F/D/E calls. Baseline-only buffering requires successful worker completion and matching committed/emitted insert counts; workload calls are not batched. F remains separate Read and Update calls. |
| sysbench / capture | Configured sbtest tables, id primary and k secondary indexes; six configured OLTP scripts including delete/reinsert and native conflict retry. |
| HyBench / capture | Eight tables, eight primary and thirteen secondary indexes; AP → TP → concurrent XP. Native transaction conflicts (DuckDB write conflicts; PostgreSQL deadlocks and serialization failures) roll back that transaction and continue, retaining source diagnostics/outcomes; other SQL errors fail. |

Source retries require matching outcomes; the generator does not invent accesses by rerunning SQL or silently retry failed SQL. Setup, execution and shutdown have separate bounds; cleanup errors cannot replace the first failure. Successful publication requires normal shutdown, not crash recovery.
