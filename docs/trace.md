# Trace contract

The native producer uses the explicitly selected engine for stored rows, transactions and capture. Both
`api` and `capture` modes publish the format below. Successful operations and
transaction-local searches remain in aborted transactions; `Abort` has the same
operation/write accounting as `Commit`, but never publishes those writes as
committed state. No legacy format reader is provided.

## Native row consumption component

Sealed native data retain current rows, committed before-images, stable RowIDs,
canonical transaction outcomes and native statement read points in one database.
The DuckDB standalone file inventory binds byte size, SHA-256, engine version/source
identity and the last resolution slot. A matching read-only reader can reopen the
file after relocation, without the capture host or data re-import. A remaining
WAL, spill dependency, checksum mismatch or different reader engine identity
prevents opening it as a sealed artifact.

PostgreSQL stores a normally stopped native cluster with a per-file size/hash inventory,
matching server/instrumentation identities and a last resolution slot. Its row reader
verifies this inventory and starts a private copy using the explicitly supplied runtime
and fresh working directory. Read-only transactions prevent business writes; the
original artifact remains unchanged. The private copy is removed after normal close.
This reader cost is separate from generation and index-only replay.

RocksDB retains timestamped rows, RowID mappings and statement read points in native files. Its inventory binds file sizes, SHA-256, runtime/bridge identities and the last resolution slot. The reader independently verifies the files and opens them read-only in place, without a mutable copy or recovery. Empty native-created WAL files are allowed; actual WAL records prevent publication.

`NativeData.snapshotRow` takes table, RowID and canonical read version.
`rowBeforeStatement` takes table, RowID and native connection/query identity; it
resolves the actual transaction snapshot plus preceding local writes. This is a
statement-start point, not a claim about arbitrary intra-statement index accesses.
Deleted identities cannot resolve to a replacement inserted under the same key.
Unknown statement points and versions beyond the published watermark are errors.
PostgreSQL records each statement's actual native snapshot, including changing RC
snapshots and fixed RR snapshots within a single transaction.

Committed local row versions are retained. Full row contents of rolled-back writes
are not: a statement following such local writes explicitly rejects row retrieval.
This does not remove the corresponding index operations or Abort from index-only
Trace replay. The native reader resolves RowIDs rather than physical Locators.
Binary fields are detached byte arrays, including empty values; SQL NULL stays null.

## Native publication

Both native input modes publish `baseline.pb.zst`, `workload.pb.zst`,
`work-units.jsonl.zst`, native files under `data/` and `manifest.json`. Generation and
native history sealing leave `INCOMPLETE` present. Once source connections and the
host close, publication verifies the standalone file and matching data/Trace
resolution watermark, embeds its inventory as `native_data`, and removes the
marker only after the manifest exists. Consumers reject the marker. Failed
compilation retains diagnostics; it cannot be consumed as a complete Trace.

Both engines emit the final baseline stream from native initial rows before workload
execution. PostgreSQL also emits the final workload stream during execution. Only pending transactions may spill temporarily.
Source sequences, native ordering watermarks and client/source completion counts
are checked during necessary reads. It seals resolution metadata after drain, stops
the server normally and publishes the cluster inventory without an independent
historical-row audit or reference replay. Business tables are UNLOGGED; this contract
requires normal server shutdown and does not provide recovery of interrupted generation.
Readers use an isolated copy of the sealed cluster and leave published files untouched.

`TraceManifest.openData` verifies the embedded native inventory and opens the
row resolver above. PostgreSQL requires the overload with explicit runtime and fresh
working directory. The manifest's `value_kinds` explicitly declares
supported index values. Native publication currently declares only `row_id`;
service replay rejects `locator` before opening or creating a backend.

The native work-unit catalog records distinct operation origins in increasing raw
`source_event` order, with actual `connection_id` and `query_id`. Capture origins
are adopted ART accesses or successful row-change facts; API origins are source
statement starts. Operations bind to their exact origin. Source statements with
no projected index operation remain counted in `source_statements`, not as
invented accesses. Canonical Trace still follows transaction resolution and
within-transaction operation order; raw log order does not define commit order.
For PostgreSQL capture, each statement is a work unit; its `source_event` is assigned
during the ordered transaction merge. Actual backend/request identities remain in
`connection_id`/`query_id`. A statement without a projected index operation remains
an origin in that catalog and never creates a synthetic access.

The complete index definitions, benchmark, source commit, schema digests and
source schema configuration are bound by `catalog_sha256`. Consumption validates
this embedded identity and its table/index structure without opening the source
checkout. The source directory is informational. Source/schema validation belongs
to generation; the digest is an integrity identity, not independent certification
of an untrusted producer's completeness.

## Protobuf format

Trace stores the Index Service request messages from
`proto/index_service.proto`, wrapped by `index_trace.proto` source
positions. A query group is one SearchGroup request with one position per member.
TraceBegin records preserve source transaction boundaries and are not RPCs.
Stored commit counts describe exported requests; replay adjusts IDs, routing,
positions and counts after filtering or batching. Native generation has one protobuf output format.

Each protobuf file starts with big-endian int32 magic `0x53495042`. Each block has
four big-endian int32 fields: message count, compressed byte count, uncompressed
byte count and CRC32C of the uncompressed bytes, followed by an independent Zstd
frame at level 3. The plain block contains length-delimited TraceRecord messages.
Blocks target 1 MiB and contain at most 4096 messages; a single request is limited
to 32 MiB including its delimiter. Four zero int32 fields terminate the file;
missing termination and trailing bytes are errors. File SHA-256 identities bind
the compressed bytes. Each inventory entry in `proto_files.baseline` or
`proto_files.workload` records `path`, physical `sha256`, `logical_sha256` of the
length-delimited uncompressed messages, and the semantic operation count (one per
group member and transaction control). The baseline/workload Trace
identity hashes the canonical file's 32-byte logical digest
and big-endian int64 operation count. No detailed-operation encoding is needed to
compute these identities. Replay and locality analysis read the protobuf requests. Readers enforce file,
logical and inventory identities, transaction positions, commit counts and
complete source-member coverage.

Each inventory entry also reports `bytes` for the complete compressed file and
`statistics` computed during writing, separately for baseline and workload:

| Statistic | Counting unit |
| --- | --- |
| `index_entries` | Index writes and search members; excludes transaction markers. |
| `effective_requests` | Each write, Search or complete SearchGroup once; excludes markers. |
| `primary_entries`, `secondary_entries` | Exported entries by index scope, including search members. |
| `request_types` | Requests by `insert`, `upsert`, `logical_delete`, `relocate`, `point_search`, `range_search`, `union_search` or `intersection_search`; absent kinds have zero count. |
| `transaction_markers` | Begin, Commit and Abort separately. |
| `write_origins` | Exported write entries carrying business-insert/update flags, not distinct source rows. |
| `uncompressed_message_bytes` | Length-delimited protobuf bytes, excluding file/block headers. |

Requests from aborted transactions count because replay executes them before
Abort. Request counts are independent of returned rows and transport batching.
These producer statistics are informational, not substitutes for digest and
semantic validation; preflight independently counts the consumed stream.

## Source facts

Capture generation binds source records to native connection, query and transaction
identities, using query, transaction, mutation, index-attempt, visible-row and
consumption logs. Query/session completion and transaction outcomes are required.
The compiler does not rerun source SQL to synthesize index accesses.

YCSB uses an embedded RocksDB API binding and emits its actual calls, status and result counts directly into the final Trace. Native timestamped rows retain history; row locks protect partial updates. A
read-modify-write workload issues separate Read and Update calls; it is not made
atomic by the generator. sysbench uses libpq or the native DuckDB driver; HyBench uses the
shared-host JDBC adapter. Source retries remain source behavior and must have
matching receipts/outcomes. Generation does not silently retry failed source SQL.
HyBench rolls back native write-conflict transactions and continues subsequent
transactions; it retains their reads, prior successful writes and Abort. Other
SQL errors still fail the source. YCSB baseline loading alone buffers upstream
inserts into bounded native transaction batches. Its immediate OK means buffered input;
generation requires each worker's successful completion, with committed rows and emitted baseline counts matching the upstream insert count. A failed batch or
cleanup cannot certify the baseline. Workload API calls are not batched.

RocksDB generation skips WAL and per-operation durability synchronization. Successful publication flushes all column families and closes the engine; native files and build identities are retained. Failure leaves diagnostics and an incomplete artifact, never a resumable generation. SST/Blob SHA-256 identities are computed during native writes and collected from the same engine after successful close; publication does not reread SST/Blob contents. Missing or differently sized SST/Blob outputs prevent publication. Consumers verify the recorded file bytes independently. History retained for snapshot reads is distinct from a crash-recovery log.

PostgreSQL consumes native capture facts online, retaining source completion/count reports and final Trace identities. It does not retain full raw facts or answer-key files. DuckDB currently retains raw facts and count-only receipts with hashes for offline binding. `source_evidence` binds the
run's effective configuration, client and engine identities and corresponding source evidence.
It records expected baseline table counts. HyBench also binds its load
input manifest, including the CSVs removed after successful loading and the two
runtime files checked before workload execution. The local engine build receipt
binds the pinned source, instrumentation, prepared sources, build commands and
executed binaries; it is not a signed or hermetic build attestation.

After DuckDB receipt/statement binding, `source-diagnostics.json` retains per-window summaries of storage SQL
outcomes, statements without adopted ART, adopted/abandoned attempts and mutation
scopes by statement outcome. It is written before access projection, so it also
survives unsupported-access failures; successful publication binds its hash in
`source_evidence`. It scans already imported facts, not benchmark row data. In API
mode these are storage SQL diagnostics, not API operation counts.

Attempt costs include probe calls, summed candidate-vector maxima and measured
probe nanoseconds, not distinct rows, physical I/O or full query time. Mutation
counts distinguish requested rows, completed scope rows and scopes without a
completion. They do not measure a failed scope's physical successful prefix.
Completed internal scopes in a failed statement remain diagnostic. A statement
without adopted ART may still maintain indexes; this classification alone does
not prove that it executed a column scan.

## Access projection

A capture Search represents a **finally adopted ART access's snapshot-visible
matches**, including earlier writes from its own transaction. It excludes later
non-index predicates, sorting, aggregation and other SQL operators. SQL result
counts therefore need not equal Search counts. Abandoned ART attempts, raw
candidate counts, physical row order and extra reads remain diagnostic facts;
the Index Service need not reproduce the physical process.

The boundary is the index operator's actual output, not subsequent consumption.
If the index has emitted its complete visible match set, a downstream JOIN that
discards the batch or a LIMIT that uses only part of it does not shrink Search.
Completeness requires all base candidates and transaction-local matches to have
been exhausted; an empty local store can establish the latter without an extra
fetch. Generic stop records do not measure consumption: their zero prefix is a
placeholder. Partial source output is rejected unless its exact range, order and
truncation can be represented; the current compiler accepts complete output only.

| Native case | Projection |
| --- | --- |
| Single-column ART equality or ordered comparison | Encode actual typed probe bounds; intersect two bounds when present. |
| Repeated point probes, such as supported IN accesses | Deduplicate points and emit a union group with the native visible result count. |
| Base plus transaction-local ART probes | Require matching conditions and preserve the actual snapshot plus prior local writes. |
| Fully drained adopted access | Search over the represented predicate, using the native visible-match count. |
| Interrupted primary-key equality after its sole matching row was output | Preserve the exact point Search and transaction outcome. Require identical equality predicates across probes and exactly one emitted primary key matching that predicate; native uniqueness proves complete logical output even without the final empty fetch. |
| Abandoned attempt or internal history-table access | No benchmark Search; retain source facts for diagnosis. |
| Downstream stop after complete index output | Preserve the complete snapshot-visible Search, irrespective of downstream consumption. |
| Incomplete index output | Reject: no exact partial-range/order projection is currently implemented. A SQL LIMIT cannot be copied into Search limit. |
| Missing identity/batch/close facts, unsupported types/conditions, or ambiguous intra-statement read/write ordering | Fail generation rather than approximate or invent accesses. |

The configured source may execute column scans with no index operation. The
compiler does not manufacture an index access for such a statement. YCSB's
explicit API Scan has its requested maximum result count and may exhaust its
range before that maximum; this does not change capture-mode early-stop rules.

A failed SQL statement may perform ART writes that are immediately undone without
forming a result visible to later operations. Those internal writes stay in failed
attempt diagnostics, not logical Trace writes. Unknown partial physical success
must not be reported as all success or zero physical work. Earlier successfully
completed writes and accurately projected Search operations remain, followed by
the actual Commit/Abort. If a read observed such internal temporary writes, its
semantics must be resolved explicitly before accepting that projection.

## Validation responsibilities

| Boundary | Required checks and cost |
| --- | --- |
| Configuration/source/build | Before any host starts: selected mode/profile, source gitlink and clean checkout, native client inventory, fresh and nonoverlapping outputs. Reads selected configuration and source/build files. |
| Native load | Per-table native row counts before workload; history/RowID installation. HyBench reuses actual COPY counts from its atomic load; subsequent index/sequence setup cannot change rows. sysbench uses stored table counts; YCSB checks upstream successful inserts, completed binding calls and native baseline-row counts. HyBench hashes generated inputs once and checks runtime files before execution. |
| Facts and projection | While binding actual receipts/logs: source completeness, statement outcomes, transaction/snapshot consistency, mutation attribution, exact supported accesses and RowID coverage. Reads captured facts; does not run a reference workload. |
| Publication | Validate common baseline/workload structure while writing; seal history and bind data/Trace watermarks after normal shutdown. RocksDB reuses same-instance write-time SST/Blob SHA-256 and hashes remaining metadata; other native engines hash closed artifacts. |
| Consumption | Manifest and catalog integrity, file/logical digests, record counts, complete expansions/source-origin bindings, requested value capability and sufficient replay supply. |
| Service replay | Request/response completion, source expected counts and exact Flush publication; see [Index Service](index-service.md#results-and-validation). |

Native baseline requests are derived from loaded initial rows and their native identities,
with every source index expanded. Production replay does not create a per-query
answer file. Matching result counts cannot establish equal keys, RowIDs, versions
or order; controlled native observations and independent small tests provide those
oracles. Full-input independent acceptance is an explicitly triggered workflow,
not an extra scan attached to every generation.

Changing client count, dispatch or backend can reuse an immutable Trace. A new
run still checks its inputs. Evidence for different bytes, configurations or
relevant implementation semantics cannot replace that check.

## Identity and versions

```text
PrimaryKey   = (Table, IndexID=0, PK, VersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, VersionDescending)
LogicalValue = RowID
```

TableIDs start at 1 in source-schema order. IndexID 0 is the primary key;
secondary IDs follow each table's source index order. The complete catalog and
its identity are embedded in the manifest even for primary-only export.
`secondary_index_coverage` is `primary_only` or `all_source_definitions`.
Benchmark setting `trace.secondary_indexes` defaults to false; true selects all
source definitions. Source database indexes are retained in either case.
Nonunique secondary predicates search
all matching primary keys rather than becoming one PK lookup.

`IndexKey` components are self-delimiting and order-preserving under unsigned
byte comparison. NULL is `0x02`; non-NULL values start with `0x01`. Signed integers
use sign-bit-flipped big-endian int64. Timestamps encode microseconds from 1970
with supported infinities at the extremes. Text is UTF-8 terminated by `0x00`;
embedded NUL is unsupported. Native DuckDB CHAR is VARCHAR: trailing spaces are
preserved. Current index families are signed integers, timestamps and binary
ordered text; arbitrary collations and composite/expression indexes are unsupported.

The backend key bytes are `Table(uint32 BE) ++ IndexID(uint32 BE) ++
[IndexedValue] ++ PK ++ (~Version int64 BE)`. A component-prefix point is
`[value, prefixSuccessor(value))`; inclusive/exclusive boundaries adjust that
prefix endpoint. Values at a read point select the latest committed version at
or below it; a selected tombstone means absent and must not reveal an older value.

Baseline version is 0. Observed source transaction outcomes receive canonical
resolution slots in native outcome order, including Commit, Abort and read-only
transactions. Native start IDs need not increase in that order. Trace
`transactionId` uses the resolution version; persistent
`sibench_history.transactions` retains its mapping to native transaction IDs.
The actual native snapshot determines the visible committed upper bound, rather
than wall-clock time, transaction ID magnitude or replay completion order.

Within a transaction, operation order and `mutationOrdinal` identify preceding
row changes. Transaction-local Search overlays only that transaction's earlier
writes on its original snapshot. Other transactions cannot read those provisional
writes. Commit makes final changes visible; Abort discards them from committed
index state. Abort still occupies its resolution slot and retains its preceding
operations in Trace. The service protocol and visibility rules are specified in
[Data transactions and reads](index-service.md#data-transactions-and-reads).

An Update keeps its RowID. Delete followed by reinsertion allocates a new identity,
so an old RowID cannot resolve to the replacement row. Failed statement internals
are not used to guess missing successful identities. Primary-key assignments,
including permutations and attempts later rolled back, are unsupported.

## Work units and routing

`work-units.jsonl.zst` stores `work_unit_id`, `connection_id`, `query_id` and
`source_event`, numbered continuously from 1 in raw origin order. Its SHA-256 is
in the manifest. Native origins are described under [Native publication](#native-publication).
This catalog associates requests with recorded operations; it does not reconstruct
a serverless DAG or make log arrival order into transaction order.

Baseline work units group 4,096 source rows across table boundaries, before index
expansion. A row's primary and secondary entries stay in the same work unit.
Control records have workUnitId 0. Workload operation origins have positive IDs.
Replay preserves the source order and uses these IDs for configured client routing.

## Operations and index expansion

| Operation | Meaning |
| --- | --- |
| Baseline Insert | Load one declared index entry at version 0. |
| Search / SearchGroup | Query native index bounds at the recorded read point; groups preserve one source access and its expected union/intersection count. |
| Upsert | Successful source row insert/update projected onto one index entry. |
| Logical delete | Versioned invalidation; no RowID/Locator sentinel is used. |
| Begin | Trace transaction boundary, not an RPC. |
| Commit / Abort | Resolve the source transaction with exact operation/write accounting. |

A row change expands to the primary index and every exported secondary index in increasing
IndexID order. Changing an indexed value deletes the old secondary identity and
upserts the new one. Unchanged secondary keys also receive the new row version.
The service must not implicitly perform this expansion again.

All members share source event, transaction/version, mutation ordinal, work unit
and RowID. `expansionPosition` is consecutive from 0 and `expansionSize` is the
complete width. Delete positions carry `deleted_row_id` for maintenance validation.
A query or control cannot interrupt an expansion. SearchGroup has one source
position per member, shared group expectations and complete member coverage.
Primary-only export rewrites mutation expansions to one primary entry. It omits
secondary searches and whole SearchGroups containing any secondary member,
without changing the expected result of a remaining query. Begin/Commit/Abort
and source mutation ordinals remain; stored request/write counts are recomputed.
Preflight validates complete expansions for the declared export scope before
applying any additional consumption-side secondary-index filtering.

Search results use canonical key order, ascending unless the request specifies
descending. A positive API limit is a maximum, with expected count no greater
than it. Snapshot and transaction-local read points are distinct from the native
engine's physical row ordering. The native producer currently supplies RowID
values; the protocol's Locator/Relocate capabilities do not imply a native data
relocation or GC workflow.

## Delivery selection

Consumption requesting secondary indexes rejects a primary-only Trace before
opening the backend. A full export can be consumed with either setting.
Baseline loading drains before workload sending. Replay applies configured
secondary-index filtering and client routing, then adjusts sent positions and
transaction counts. The Index Service owns visibility; replay adds no synthetic
read barriers or source SQL retries. A request failure stops sending, cancels
outstanding work and closes clients.

Reports distinguish baseline/workload records, effective operations, source
transactions, commits/aborts, responses, in-flight work and backpressure.
`last_resolved_version` includes both committed and aborted transactions. RowID
replay does not rewrite native row files. The [service contract](index-service.md)
defines acknowledgment, Flush and durability.

## Completion and failure

Only normally closed, independently readable native data may be published.
Compilation leaves `INCOMPLETE`; successful publication atomically writes the
manifest before removing it. Consumers reject incomplete or altered artifacts.
Generation retains the first failure and diagnostics and stops later benchmarks.
There is no PG fallback, resume, recovery replay or automatic source retry.

Source clients have a configured execution timeout. Host setup and shutdown are
bounded separately. Cleanup errors must not replace the original error. A correct
final database alone cannot certify complete capture: missing receipts, unresolved
transactions, scan fragments or snapshot mismatches still reject publication.

## Replay preflight

`check-replay CONFIG TRACE` validates input without invoking an adapter. Replay
already performs preflight, so this is a standalone diagnostic, not a required
extra step. Preflight checks digests, record/expansion counts, source bindings,
client coverage and operation supply. Runtime value capability is checked before
creating/opening the target backend.

Supply budgets count effective operations after filtering, excluding baseline and
controls; each SearchGroup counts once, including requests in aborted transactions.
The budget is warmup operations + ceil(rate × measurement seconds × (1 + margin)).
Replay dispatch timing currently uses target calls, including terminal markers
and individual group members; its `warmup_calls` and call rates are not effective
request rates. The supply budget alone does not normalize that timing window.
Insufficient supply fails before loading/sending. Supply does not establish
achievable throughput or sustained scheduling. Generation emits a finite Trace;
segmented resume/checkpoints are unsupported.

## Source scope

Only pinned clean benchmark submodules and the supported native schema/configuration
are accepted. Generation creates that schema in a new private host; dynamic DDL
and arbitrary external databases are not supported.

| Source/mode | Data and indexes | Execution |
| --- | --- | --- |
| YCSB / api | UTF-8 key, configured binary fields in native timestamped RocksDB rows; one primary index. | Upstream A/B/C/F/D/E through the API binding. |
| sysbench / capture | Configured sbtest tables, id primary index and k secondary index. | Six configured OLTP scripts, including delete/reinsert and conflict retry. |
| HyBench / capture | Eight source tables, eight primary and thirteen secondary indexes. | AP → TP → concurrent XP; access projection follows the complete-output boundary above. |

The baseline count must match each configured source table independently. HyBench
uses its upstream parameter table for the selected `sf`; the small integration
checks have separately bound inputs. Supported mutation and access semantics are
those stated above, not arbitrary SQL support implied by DuckDB itself.

## Data files

One sealed `data/data.duckdb` holds current rows, committed before-images, RowIDs
and source transaction/read-point metadata. DuckDB controls its native file layout;
the selected native database owns loading, history and capture throughout generation.
Logical historical versions are deliberately retained. Rolled-back full row images
are not preserved for Locator retrieval, as described under native row consumption.

Rewrite, physical version GC and file relocation maintenance are not implemented.
Business Delete is versioned invalidation; it does not reclaim history. Backend
SST/WAL/MANIFEST files are separate index state controlled by that backend, not
row-data file generation settings.
