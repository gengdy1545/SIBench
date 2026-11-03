# Trace contract

## Output strategies

Benchmark `capture.output` defaults to `proto`. Both modes use the same source checks, transaction/snapshot projection, index maintenance and native counts.

| Output | Artifacts |
| --- | --- |
| `proto` | Complete baseline and incremental Parquet, `baseline.pb.zst`, `workload.pb.zst`, work units, access ledger, completeness report, manifest and source identities |
| `full` | All proto artifacts, plus compressed raw facts and `workload.ops` |

Baseline requests derive from actual Parquet layout. Committed inserts and updates receive their written FileID and zero-based row position. Both modes support RowID/Locator values and secondary-index filtering. Publication retains row data; failures retain diagnostics.

PG and JDBC source streams require explicit completion, matching identities and
continuous per-backend transaction sequences. Projection waits for earlier writer
facts before completing a transaction; readers tied at one completion position
follow receiver-arrival order while preserving session order and share the same
visible writes. Source backends must hand off their final facts, including
rollback on connection close, before exit.
Missing completion fails generation. Generation uses no Index Service and adds
instrumentation, transport and projection overhead to native execution.

`generation.pending_transaction_bytes` and `generation.pending_transactions`
limit pending encoded facts, including completed transactions awaiting source
order. Bytes are measured before spool compression. Overflow fails generation
without dropping events or publishing partial transactions. Consumed scratch is
reclaimed; failures may retain unprocessed diagnostics. These limits do not cap
Java object overhead, audit state, RowID storage, PG memory or total disk use.

Optional `generation.operation_target` counts emitted workload Trace logical
records, including transaction-control records and index expansion.
`generation.output_byte_target` counts physical bytes of all canonical proto
files, including Baseline, excluding metadata, Parquet and diagnostic side files.
Omitted or zero targets are disabled. An output target no greater than the
complete Baseline proto size is rejected; initial-state-only captures are unsupported.
These are soft targets: after a projected complete transaction reaches a target,
native clients stop starting new business operations and finish active operations.
Produced source facts are still drained, projected and audited. Projection lag,
active transactions and block buffering can overshoot the target. Target stops
skip later native workloads/phases and certify the actual executed prefix, with
the requested native budgets retained alongside actual native counts. The
`generation_budget` entry binds request-time progress, final counts/bytes,
overshoot and stop reason. It is not a full-input or hard resource-limit claim.

Request auditing requires agreement between result-consumption counts, server execution
and transaction association before releasing state. Connection close seals
remaining results. Pending-request and unassociated server-event limits fail
generation on overflow; they are not whole-process memory caps.

## Protobuf format

Trace stores the Index Service request messages from
`src/main/proto/index_service.proto`, wrapped by `index_trace.proto` source
positions. A query group is one SearchGroup request with one position per member.
TraceBegin records preserve source transaction boundaries and are not RPCs.
Stored commit counts describe unfiltered requests; replay adjusts IDs, routing,
positions and counts after filtering or batching. Full output adds detailed
operations from the same projection.

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
group member and transaction control). Both output modes' baseline/workload Trace
identity hashes the canonical file's 32-byte logical digest
and big-endian int64 operation count. No detailed-operation encoding is needed to
compute these identities. Detailed operations are diagnostic side output;
replay, locality analysis and the full-row oracle read the protobuf requests. Readers enforce file,
logical and inventory identities, transaction positions, commit counts and
complete source-member coverage.

## Source facts

The generator observes requests, parameters, result counts, index candidates, row changes and transaction outcomes from native execution. Projection converts supported accesses into canonical requests with source-event associations; it does not rerun SQL. PG page splits, HOT and VACUUM are not downstream KV operations.

The [source-scope check](#source-scope) verifies the actual schema and indexes before capture/export. The common schema-driven projection orders source transactions by PG completion positions, including overlapping sessions and sequential workload phases. It maintains every declared index and either projects each access under the rules below or fails generation.

Full output stores `pg-<pid>.jsonl.zst` per backend and `jdbc-*.jsonl.zst` for
JDBC clients. These streams contain the same live facts consumed by projection.
PG streams start with `capture_start` and declare `detail=summary` (default) or `rows`;
JDBC streams start with `connection`. `capture-run.json` binds stream identities,
logical event digests and audits, plus physical file digests for full output.
Raw diagnostics require a complete capture manifest, one JSON object per line
and completed zstd frames. Exact replay uses the published Trace, including its
recorded ordering of concurrent readers; no production raw-to-Trace command exists.

## Access ledger

`access-ledger.json` attributes source events and reports the projection decision
for every access unit. One index rescan is one access unit, including inner-table
rescans in nested loops. Event categories must account for every input event
exactly once. Candidate index entries, visible index rows and SQL result rows
measure different stages of one access and must not be added together.
BitmapOr reports visible matches per input; BitmapAnd reports one visible
intersection for the inputs actually executed. The ledger establishes attribution,
while source-completeness checks establish the required execution coverage.

The report groups access paths by source request template, plan node, index,
scan kind and projection decision. Template identity is PG `query_id` encoded as
a string; normalized SQL is display text. It reports plan/text variants and
rejection/exclusion reasons. Any rejected unit sets `status=has_rejected_units`.
A request with no index access is recorded as such, without inferring access from
its plan. The explicit `access-ledger` diagnostic reads retained full-output facts
without connecting to PG.

Planner index-endpoint probes (`IS NOT NULL` keys outside the executor), rescans that never fetch rows, and uniqueness checks on write paths are not queries. The first two are identified and reported as exclusions. Uniqueness checks occur inside index `aminsert` and count only as write maintenance. `index_insert_attempt` records physical index-write attempts; the number per update varies with page space and is not the number of logical key-maintenance operations, which are expanded from row changes. The ledger proves that events are attributed exactly once, not that no events were lost. Residual filters above an index scan do not change the index result and are excluded.

## Access projection

`AccessProjection` produces queries or a reason for every access unit, treating BitmapAnd as a group. Ledger reporting and compiler emission share this rule. The principle is: **native execution is fixed; projected queries must reproduce its actual result set, and every expected count must be a native fact**. When one interval cannot express an access, it is split into multiple executable KV queries without approximation or rejection.

| Case | Decision |
| --- | --- |
| Scan keys (=, <, <=, >=, >) on one indexed column intersect into an encoded-value interval. IS NULL / IS NOT NULL are intervals in NULLS LAST order; no keys means a full index scan. | One query: equality / range / full_scan. |
| `= ANY(array)`: one point per distinct non-NULL element within the other keys' interval. | Multiple points form a `union` group whose expected count is the visible-row count. One point becomes one query. All-NULL arrays, which PG does not search, are excluded as `contradictory_scan_keys`. |
| Ordinary or index-only scan reaches the end of the index. | Expected count is the visible-row count, without a limit. |
| Consumer stops a primary-key scan early (LIMIT, semijoin). | Primary keys are unique, so consumed rows are a prefix in KV order: limit equals the consumed visible-row count, in the scan direction. |
| Consumer stops a secondary-index scan early. | Nonunique btrees order equal values by heap TID; KV orders them by PK, so selected rows at the cutoff are not a PK prefix. Split into `stopped_*`: the fully read interval before the last value, with its visible-row count, plus a pinned lookup for each returned row at the last value ((value, PK), expected count 1). Split array keys element by element. Every returned row needs an identity; reject `stopped_scan_without_row_identity` when all-visible index-only rows lack one. |
| Bitmap scan (single index, BitmapOr). | Project only when every candidate TID has a heap verdict. Expected counts are candidates judged visible, counted separately for each BitmapOr input. |
| Flat BitmapAnd. | An `intersection` group with one interval query per input. The group expects the intersection rows judged visible by the heap. A round with one input becomes that input's query. |
| Lossy pages, early-stopped heap scans with unjudged candidates, nested combinations, or BitmapAnd inputs with array keys. | Reject as `bitmap_heap_lossy_page` / `bitmap_heap_stopped_early` / `nested_bitmap_combination` / `bitmap_and_input_with_array_key`. |
| Row comparisons, composite keys, comparisons with NULL constants, key types outside the operator family, or array inequalities. | Reject as `row_comparison` / `composite_key` / `null_comparison` / `unsupported_key_type` / `array_inequality`. |
| Text intervals other than point lookups when database lc_collate is not C/POSIX. | Reject as `text_interval_requires_c_collation`; equality is unaffected by collation. |
| Planner endpoint probes or rescans that never fetch rows. | Exclude as non-workload accesses. |

Rejections distinguish missing source facts from key shapes outside the supported scope.

Keys are encoded according to the indexed column type. Scan keys must belong to that column's btree operator family: int2/4/8 for int, timestamp and date for timestamp, text/varchar for varchar/text, and bpchar for CHAR. Source schema preflight requires text columns and indexes to use the database's default collation, so `lc_collate` determines whether text intervals preserve order. Native capture databases for all three sources use C collation. Projection describes the index access itself. Templates whose aggregates, sorting, DISTINCT, or LIMIT make returned counts differ from visible counts retain both. Query expectations use visible counts rather than returned counts.

## Validation responsibilities

| Boundary | Required checks |
| --- | --- |
| Load/export | Pinned source/build and actual schema; per-table counts in the shared initial snapshot; emitted row coverage and closed Parquet file identities. Writers must be quiescent during export. |
| Capture/projection | Request/session completion, returned/consumed counts, transaction/snapshot consistency, row changes and index-access attribution. The normal entry point requires PG counter snapshots and rejects ledger mismatches before publication. |
| Trace consumption | Complete manifests, source bindings, file/logical digests, record counts, complete index expansions and source work units. |
| Service replay | Capabilities, ordering, native result counts, response completion and exact Flush publication; see [Index Service](index-service.md#results-and-validation). |

`baseline-manifest.json` binds schema, planned row counts, file inventory and SHA-256 identities; Trace binds it through `baseline_manifest_sha256`. Data files are hashed after close and verified before baseline derivation. Integrity checks hash actual bytes; unchanged size/timestamps are insufficient, and digest reuse is limited to the same verification pass.

Trace records query bounds, read points, transaction positions and native visible-result counts, with no per-query answer files or digests. Capture retains row identities needed for exact projection, but does not traverse or fetch rows solely to prepare answers. SQL results retain returned/consumed counts without row values: exhausted JDBC results must match the server count, while early close permits a consumed prefix. Equal counts do not establish matching keys, versions, values or order.

Independent row audits are explicit: `audit-baseline` compares source columns before the workload; `audit-rows` compares retained transitions and the quiescent final database. See [README](../README.md#independent-source-acceptance) for prerequisites and costs. Normal generation records `verification.mode=runtime` and requires neither audit reports nor reference replay. Small independent models test row identity, snapshots, Parquet content and index expansion without creating a full-run answer workflow.

Changing client count, dispatch or backend can reuse an immutable Trace; the actual run still checks its input identity. Prior-run evidence cannot replace these checks. Value comparisons follow source types: CHAR ignores trailing ASCII spaces; VARCHAR/TEXT preserve them; REAL uses single precision; dates/timestamps retain their typed semantics.

## Identity and versions

```text
PrimaryKey   = (Table, IndexID=0, PK, CommitVersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, CommitVersionDescending)
Locator      = (FileID, FileRowPosition)
LogicalValue = RowID
```

- IndexID=0 identifies the primary index. TableIDs start at 1 in source-schema table order: the fixed eight-table order for HyBench, sbtest1..N for sysbench, and usertable for YCSB. Secondary IndexIDs follow each table's source `CREATE INDEX` order. `IndexCatalog` is generated from the source schema. The manifest records source provenance (benchmark, source directory and commit, schema-file SHA256, and YCSB / sysbench native configuration) and complete definitions. Preflight reconstructs and checks them individually, rejecting self-declared partial inventories. transfer indexes 1/2/3 are sourceid/targetid/type respectively. Nonunique secondary predicates remain prefix queries rather than a single PK Get.
- PK and IndexedValue are self-delimiting, order-preserving `IndexKey` components. Unsigned byte order matches PG btree's default ascending, NULLS LAST order, with text restricted to C collation. Concatenated components compare as tuples. NULL is `0x02`; non-NULL components start with `0x01`. int2/int4/int8 use a sign-bit-flipped big-endian int64. Timestamps encode microseconds since 1970 the same way, with -infinity / infinity at the extremes; BC dates and submicrosecond precision are rejected. Text is UTF-8 terminated by `0x00`, since PG text cannot contain NUL. CHAR first trims trailing spaces to match bpchar comparisons.
- KV byte layout: `Table(uint32 big-endian) ++ IndexID(uint32 big-endian) ++ [IndexedValue] ++ PK ++ (~CommitVersion big-endian)`. A point lookup for v scans `[v, succ(v))`. An inclusive lower bound uses `v`, an exclusive lower bound uses `succ(v)`, an inclusive upper bound uses `succ(v)`, and an exclusive upper bound uses `v` as the exclusive endpoint. `succ` is `IndexKey.prefixSuccessor` and works for any following PK and version bytes.
- SIBench increments CommitVersion in original session order for every committed transaction that executed a request, including read-only commits. Baseline=0, and all changes within a transaction share its version. Transactions committed for connection setup and capture request labels (`SET application_name`) execute no requests, receive no version, and are reported as `utility_only_transactions`. Each YCSB autocommit request therefore advances exactly one version. Business timestamps, wall clocks, XID magnitude, and downstream completion order are not used.
- Sequential multi-session capture requires `session_order=sequential` in `capture-run.json`. Each native workload runs in its own client process; the next starts only after that process exits and no capture backend remains. Sessions are concatenated in workload order with continuous versions and work units. This order comes from orchestration facts, not PID or filename inference.
- Concurrent sessions use PostgreSQL transaction-completion positions, captured at the snapshot-visibility boundary. Writing transactions have distinct positions; readers without an XID share an observed position and follow its writer. Session-local order is preserved; write positions strictly increase and reader positions never decrease. Colliding writers, incomplete transaction fragments or inconsistent snapshots fail projection. Wall time, PID and XID magnitude do not establish commit order.
- Rollback does not advance CommitVersion, allocate a RowID, change committed index state, or write incremental Parquet. Later transactions continue from the latest committed state.
- transactionId is the ordinal of the source commit event. A change's mutationOrdinal is its row-change ordinal within the transaction, shared by secondary expansions. Queries in the group carry the same transactionId, with mutationOrdinal indicating how many row changes preceded the query. Both fields support source association and order checks; neither is a KV version suffix. The service adapter derives execution positions from the sent operations, and the service tracks transaction ordering and complete-commit publication as defined in the [Index Service contract](index-service.md#data-transactions-and-reads).
- ReadTimestamp represents the query's original statement snapshot, expressed as a visible-commit upper bound. At T, select the largest version `<=T` for a key. An absent version or a selected deleted version means the entry does not exist; readers must not skip tombstones to return older values.
- **Transaction-local reads** preserve the original statement snapshot and carry the source transaction's CommitVersion (SEARCH.version) and mutationOrdinal, with TRANSACTION_LOCAL_SEARCH set. Results start from the latest snapshot state `<= ReadTimestamp`, overlaid only with that transaction's writes at this version before mutationOrdinal. Other transactions committing between these positions remain invisible. Send in original position order: earlier changes → query → later changes. After the transaction ends, only the final effect for each key and version remains. Readers require `version>0` and `readTimestamp<version` for transaction-local reads.
- In sequential sessions, queries before a transaction's first change use the previous commit's ReadTimestamp. Concurrent queries use the last writing commit visible to their statement snapshot's counter. Read-only commits do not change data and are not read points. Each scan event records the snapshot counter, xmin, xmax, and xip. The compiler selects the last writing commit whose position is at most the snapshot counter as ReadTimestamp; it may precede the commit immediately before the query's send position, and SNAPSHOT_SEARCH is set. For each scan, the compiler cross-checks that commits considered visible by the counter do not appear in that snapshot's own xip. A conflict means one ReadTimestamp cannot express the visible set, causing the entire compilation to fail rather than approximate it. Snapshots marked `suboverflowed` cannot cross-check individual subtransactions; those checks are omitted and reported separately. Raw xmin/xmax/xip retain their visibility meaning and are not used directly as commit versions. Explicit historical queries select available historical points; future read points are rejected.
- UPDATE preserves RowID; reinsertion after DELETE allocates a new RowID. Rewrite changes neither the logical version nor RowID.
- FileIDs start at 1 and are never reused; positions within a file start at 0. Datasets are isolated, and int64 values are parsed without loss.

## Work units and routing

`workUnitId` identifies one audited top-level SQL execution (a JDBC/PG pair or server request ordinal). IDs begin at 1, assigned at `executor_start` before rollback/filtering. All scans, nested queries, row changes and index expansions in the request share that ID; a request is never split to create more units.

`work-units.jsonl.zst` stores `work_unit_id`, `application`, `pid`, session-local `request` and request-start `source_event`, continuously numbered across sessions in ID order. Its SHA-256 is in the manifest; operations retain `sourceEvent` and `workUnitId`. Requests without effective operations remain in this inventory but do not count as runtime sends. SQL work units approximate tasks; they do not reconstruct a serverless DAG or physical topology.

Routing is fixed: `clientId = (workUnitId - 1) % client_count`. One unit uses one client, while a transaction may span clients. Changing client count changes connections, preserving Trace order, versions and Parquet. Baseline uses a separate load identity per 4096 consecutive rows, shared by each row's index expansions.

`client_count` defaults to 1. After filtering, preflight requires effective runtime operations on every configured client; baseline coverage cannot substitute. Insufficient coverage fails before loading/sending. Capture more native work rather than repeating Trace or inventing units. Reports show configured/used clients and per-client operation, query, write and work-unit counts; coverage does not establish balance or throughput.

## Detailed operation format

The file header contains three little-endian int32 values: zstd file-type marker `0x53494f5a`, fixed record-prefix length `128`, and maximum records per block `4096`, totaling 12 bytes. Each following block stores compressed bytes, record count, CRC32C, and decompressed bytes, each as int32, followed by zstd data. The canonical stream SHA-256 uses the logical uncompressed form: file-type marker `0x53494f50`, then each block's decompressed bytes, record count, and CRC32C, followed by uncompressed records.

Each record has a 128-byte fixed prefix and four variable key components. Blocks contain at most 4096 records, using little-endian fields and CRC32C. Prefix fields are sourceEvent, kind, table, index, flags, version, file, position, rowId, expectedCount, transactionId, mutationOrdinal, readTimestamp, workUnitId, limit, expansionPosition, expansionSize, then four int32 lengths for key (PK), indexedValue, lower, and upper. Length -1 means absent; each component is at most 32,768 bytes. Component bytes follow in that order. Write records carry key and, for secondary writes, indexedValue, with NULL itself encoded as a component; they omit lower/upper. SEARCH carries lower/upper, with a missing endpoint meaning unbounded. A pinned secondary lookup also carries its PK in key. Decoding rejects corrupt lengths, malformed components, invalid UTF-8, and trailing bytes. table ranges from 1–65535 and index from 0–1024. `workUnitId` is 0 for control records and positive for compiled runtime / load operations.

| Operation | Semantics |
| --- | --- |
| INSERT | Baseline index loading. |
| SEARCH | Ordered query: primary indexes compare PK; secondary indexes compare IndexedValue. Results are ascending by (IndexedValue, PK), or descending with DESCENDING. With limit>0, return the first limit visible entries and require expected count=limit. `readTimestamp>=0` identifies the original statement snapshot. Ordinary / snapshot queries use version=0; TRANSACTION_LOCAL_SEARCH uses the transaction's CommitVersion, which must exceed readTimestamp. Flags: SNAPSHOT_SEARCH=1, TRANSACTION_LOCAL_SEARCH=4, LOWER_INCLUSIVE=8, UPPER_INCLUSIVE=16, DESCENDING=32, INTERSECT_GROUP=64, UNION_GROUP=128. **Pinned lookup**: a secondary point interval with a PK in key names exactly one (IndexedValue, PK) entry, expects 1, has no limit, and belongs to no group. **Query groups**: consecutive INTERSECT_GROUP or UNION_GROUP queries represent one source access, with `expansionPosition` 0..n-1 and `expansionSize`=n≥2. Members share sourceEvent, table, readTimestamp, version, transaction position, work unit, and group expected count. Intersection members may use different indexes of the same table; union members are points on the same index. The expected count is the PK intersection / union of member results. Individual member counts are not source facts and are not checked separately. |
| UPSERT | Write with a commit version. Flags: business INSERT=1, UPDATE=2. |
| LOGICAL_DELETE | Explicit versioned invalidation, without using a valid Locator / RowID as a sentinel. |
| RELOCATE | Changes the location of an exact version of the complete index key; deleted or absent versions are rejected. |
| TRANSACTION_BEGIN / COMMIT | Operation-group boundaries for a committed source transaction, with TableID=0. Consumed by the driver only, never sent to the target index. |

Non-SEARCH operations have readTimestamp=-1. `IndexMutation` explicitly expands each row into the primary index and every secondary index of its table. The sender must not maintain them implicitly again.

Canonical Trace preserves the original relative order of queries and changes within committed source transactions. Queries occupy the native scan-start position, with candidate counts filled in at scan end. Each change writes the primary index first, then expands secondary indexes in increasing IndexID order. An indexed-value change writes LOGICAL_DELETE for the old `(Table, IndexID, IndexedValue, PK)` at the same commit version, followed by UPSERT for the new identity. Secondary entries whose keys are unchanged also receive UPSERT to refresh the row version and Locator / RowID. This neither changes index definitions nor deletes business rows or physically removes historical versions.

Baseline emits INSERT per index; runtime INSERT emits UPSERT per index; DELETE invalidates the primary and all secondary entries. Reinserting the same PK allocates a new RowID; UPDATE retains the original RowID. Old entries, new entries, and the primary entry share source event, version, transaction, mutationOrdinal, workUnitId, and RowID. Value locations agree except that deleted entries have no physical location. `expansionPosition` is consecutive from 0, and `expansionSize` is the complete expansion width. Queries and control records cannot interrupt an expansion. Query groups reuse these fields for group position and width and likewise cannot be interrupted. Before secondary-index filtering, preflight checks the full inventory, complete expansions, and source work-unit bindings, rejecting truncation or missing members.

During generation, incremental files are created lazily for actual tables as `<table>-versions.parquet`, with physical row locations. Each write retains `_sib_timestamp`, `_sib_mutation_ordinal`, and `_sib_row_id`. The same key and version overwrite in order. Intermediate effects serve original transaction-local queries without creating additional commit versions; later source transactions see only the final committed effect. Primary-key value UPDATE is explicitly rejected as out of scope. Incremental files roll after a closed row group reaches the source configuration's `data_layout` target size, independently of replay-backend SST settings; see [Data files](#data-files). The first FileID is the Baseline maximum plus TableID. Later FileIDs increase globally beyond the initial-file range reserved for all tables, using full int64 values. Actual row counts and SHA-256 are checked per file, and Locators point to actual files and positions.

## Delivery selection

Projection emits a transaction's index operations and incremental rows only after its commit. Rollback emits neither writes nor compensating operations. Queries before an aborted transaction's first change remain; later queries are excluded because they depend on omitted writes. This rule does not infer dependencies from query keys. Unfinished transactions fail generation.

The manifest records `change_delivery=committed_only`, raw/discarded row-change counts, `logical_row_version_writes`, `index_version_writes`, `transaction_local_searches` and `discarded_aborted_transaction_local_searches`. `aborted_transactions` includes explicit rollback and connection-close aborts. PG `index_insert_attempt` counts physical attempts, not downstream writes.

[Runtime configuration](../config/sibench.yaml) owns secondary-index filtering, dispatch, clients and timeouts. Disabling secondary indexes filters every operation with IndexID>0, preserving primary identity, source order, versions and Parquet. A query group is filtered as a whole if any member uses a secondary index; no replacement queries are introduced.

| `dispatch_mode` | Driver input unit |
| --- | --- |
| `operation` (default) | Individual operations after validating their complete source group |
| `transaction` | The ordered operations of one committed transaction, routed individually and potentially across clients |

Both modes send the same sequence: no merging writes, moving queries or downstream SQL transactions. The driver validates the complete source group and its closing COMMIT before sending; the group limit is 1,000,000 expanded operations, including filtered members. Trace BEGIN/COMMIT are driver controls; the adapter builds service commit markers with actual sent operation/write counts, even for transactions emptied by filtering.

One sending thread follows Trace order, waiting for each local transport acknowledgment before the next send. Buffering a batch/group member is not an RPC. Checked responses can complete out of order; the [service](index-service.md#interfaces-and-acknowledgment-boundaries) owns transaction execution and visibility. The driver adds no visibility barriers or retries. Historical read points may precede the previous sent commit.

`max_in_flight` bounds outstanding responses (default 4096); sending waits at the limit and drains at completion. `request_timeout_seconds` covers send and response waiting, accepts 1–86400, and defaults to 60; the public profile sets 600. Adapters must return send handles promptly, enforce transport timeouts and cancel resources on close. Failure stops sends, closes clients and cancels outstanding futures. Workload sending starts only after baseline loading drains.

Reports separate persisted records/controls, source operations, filtering, sends, source transactions and operation kinds. `target_calls` counts logical adapter calls, not RPCs, including separately reported `commit_markers`. `source_dispatch_groups` counts nonempty groups only in transaction mode. Reports retain the client-count-independent `operation_order_checksum`, response counts, peak in-flight requests and backpressure time. `sending_threads=1`, `service_visibility_owner=index_service` and `downstream_transaction_api_required=false` declare ownership. RowID-mode relocation emits no index writes. Multi-sender replay and physical version GC are unsupported.

## Completion and failure

Baseline export retains `INCOMPLETE` until its Parquet, file inventory and identity manifest are complete. Trace publication likewise requires a complete manifest and checksums. Consumers reject incomplete or modified artifacts.

Native execution writes `capture-run.json` in the source-control directory selected by `capture.raw_directory`:

- `running`: incomplete and cannot be published.
- `failed`: retains the first error, SQLSTATE, request / transaction context, and stack trace. It cannot be published or resumed.
- `complete`: connections closed normally, and correlated same-execution audits
  of requests, result counts and transaction boundaries passed. `sessions` binds stream
  identities, logical event digests, binding checks and audit certificates;
  full output also binds the retained compressed files. `build` identifies the
  generator, instrumented PG, trigger library and pinned native client. Normal
  generation consumes live events once; explicit raw diagnostics verify their
  retained file inventory and recorded certificates before reading them.

Normal generation takes source index-counter snapshots around the native workload
and checks its ledger against PG14 counter semantics before publishing
`capture-completeness.json` and the final manifest. A mismatch fails and retains
`INCOMPLETE`. The manifest records `access_ledger_sha256`, `capture_completeness`
and `business_changes_by_table`. Proto file digests are computed while writing.

JDBC exceptions fail the current run even if native methods catch them. Logging or cleanup exceptions do not replace the original cause. Auditing does not prove global freedom from lost events.

The pinned sysbench PostgreSQL driver rolls back and retries its recognized
deadlock, unique-violation and serialization errors. Generation requires its
ignored-error count to equal both server aborts and aborted executions. Successful
statement counts exclude those failed executions; explicit-transaction retries
add counted BEGIN calls, while the driver's direct ROLLBACK bypasses its counters.
Missing returns without an abort, inconsistent counts and other source failures
still reject the capture. Retrying belongs to the native driver; SIBench does not
retry source SQL.

After a generation failure, diagnose the retained first error and rerun in a fresh
directory. Open transactions require server end boundaries; connection/file close
cannot silently discard them. Mismatched transaction identities, unfinished
scans, missing completion positions, snapshot conflicts and incomplete expansions
prevent publication. Correct final state alone does not establish event completeness.

## Replay preflight

Generation produces a finite Trace; resumable segments and checkpoints are unsupported. Preflight checks supply before replay, without promising achievable throughput or sustained scheduling.

Budgets count effective runtime operations after secondary-index filtering, identically for both dispatch modes, excluding Baseline, controls, and derived probes: warmup operations + ceil(rate × measurement seconds × (1 + margin)). Insufficient supply is rejected before loading / sending. Preflight establishes input quantity, not achievable throughput or sustained scheduling.

`check-replay` validates the manifest, digests, pinned source-index inventory, complete expansions, source work-unit bindings, format, record counts, CRC, filtered client coverage, and budgets without invoking an adapter. Replay and locality analysis require a complete manifest with nonnegative int64 `baseline_rows`, `source_work_units`, `baseline_operations`, and `workload_operations`. Production replay requires directory-level preflight.

## Source scope

Only pinned, clean benchmark submodules and their declared source layouts are accepted. Preflight is a read-only point-in-time check. It neither creates missing indexes nor detects external DDL drift throughout execution.

| Source | Tables and indexes | Native execution scope |
| --- | --- | --- |
| HyBench | Eight tables, 8 single-column primary keys, and 13 ordinary single-column secondary indexes from the fixed DDL. | runHTAP AP / TP / XP; freshness DELETE is not called by default. |
| YCSB JDBC | VARCHAR primary key and the configured number of TEXT fields; no secondary indexes. | Native A / B / C / F / D / E; E uses readallfields=false. |
| sysbench PG | Configured table count, each with an id primary key and k secondary index; auto-inc can be enabled or disabled. | Six oltp scripts, including DELETE / reinsertion; standard scripts do not query by k. |

The actual table inventory, complete column order / types / widths / nullability / defaults, and primary / secondary index definitions must match the source. Sequence-default references and ownership must also match. Extra tables, views, temporary tables, partitioning / inheritance, RLS, generated columns, extra unique constraints, composite / expression / partial / INCLUDE indexes, custom operator classes / collations / ordering, and non-B-tree access methods are rejected. Ordinary logged and unlogged tables are supported by schema validation; native generation requires unlogged source tables. The configured database, role, version, port, isolation, and parallelism settings must match the actual connection. Text intervals also have the C/POSIX collation restriction above.

Primary-key value UPDATEs, including rolled-back attempts, are out of scope. Ordinary-column / secondary-index-column UPDATEs and DELETE / reinsertion with the same PK are supported. Schema checks do not establish row counts, current sequence values, or native-load correctness. Baseline export separately compares each table count in its shared snapshot with the native configuration (HyBench uses the pinned scale parameters); missing rows or balancing errors between tables are rejected.

## Data files

The byte policy below applies to baseline and incremental Parquet row-data files
in both output modes. Protobuf Trace files do not rotate at this
target size. PG relation files and native client load files follow their own
storage layouts.

- Baseline and incremental data share the same Parquet byte policy, configured under the native profile's `data_layout`. `target_file_size_bytes` is a positive integer, with examples defaulting to 512 MiB. `file_size_max_overshoot_percent` defaults to 5 and accepts integers from 1–100. Complete closed files include compressed pages, page indexes, and footers. Normal files range from the target to `target + floor(target × percent / 100)`.
- Each table write stream allows at most one below-target EOF tail file, which must still respect the upper bound. At most 32,767 row groups are allowed. Oversized rows, unsatisfiable group / page budgets, and metadata limits fail explicitly without padding, splitting rows, or relaxing overshoot.
- Maintained templates use parquet-java, Zstd level 3, single-threaded compression, 1 MiB pages / dictionaries, and enabled dictionaries. Supported codecs are zstd / snappy / none, with level=0 for the latter two. Buffer budgets are not hard JVM RSS limits, and the default free-disk stop threshold is not a capacity guarantee.
- Baseline assigns consecutive RowIDs in table / primary-key order within one exported snapshot. FileID is `table ordinal (starting at 1) + file ordinal (starting at 0) × table count`, within each table's output sequence.
- Changing the target or layout requires fresh output and corresponding Locator / Physical Trace. Replay does not recut Parquet. Generation exports a fresh Baseline and rejects existing output directories.
- Physical version GC is not implemented. Business DELETE publishes versioned invalidation; physically deleting the latest entry must not resurrect older values. Disabling historical queries or filtering secondary indexes does not reclaim historical versions.
- Relocation semantics have independent test fixtures; no production Rewrite / GC lifecycle is implemented.

Backend SST / WAL / MANIFEST files are index state controlled by the engine. The data-file target above is not an SST size setting. See the [Index Service contract](index-service.md) for service acknowledgment and durability levels.
