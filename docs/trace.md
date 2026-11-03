# Trace contract

## Output strategies

Benchmark `capture.output` defaults to `proto`. Both `proto` and `full` use the
same source checks, transaction/snapshot projection, complete index maintenance
and native expected counts. This setting selects artifacts, not workload
semantics or a resource/operation stop budget.

Proto-only output contains baseline shards under `baseline/`, `workload.pb.zst`, work units,
the access ledger, completeness results, the Trace manifest and source identity
snapshots. Its initial export reads indexed columns only and directly produces
final request shards. Compilation reads them to establish row identities and
retains their encoded bytes, using hard links on the same filesystem; it does
not reconstruct or re-encode baseline requests. During native execution, source
facts stream into the Java semantic projection, which writes final workload
requests without a whole-run raw file or a later conversion stage. Proto mode
writes neither detailed operations nor incremental row Parquet. The published
Trace does not reference the consumed export directory; the production script
releases that export and source control metadata only after successful publication.
Failed generation keeps diagnostics. Source manifests retain the recorded input identities, not the raw
data required for a later independent full-row audit.

PG and JDBC source streams end with an explicit completion marker; missing
completion, mismatched identities or a skipped per-backend transaction sequence
fail generation. PG exposes its completion counters and receiver acknowledgement
positions under the same source lock. A completion is eligible only after all
earlier writer facts are available. Readers tied at one counter follow receiver
arrival order while preserving their session order; these readers share the same
visible committed writes. A source backend completes its final handoff before
exiting, after PostgreSQL has rolled back any transaction still open at connection
close. Successful final handoff retires its watermark registration; an exited
source cannot hold back a later capture. There is no per-transaction receiver-confirmation wait in the native
client, and no Index Service participates in generation. Instrumentation, transport
I/O and concurrent generator work still add execution overhead.

Source-fact scratch stores only pending transactions. Encoded facts share a
64 MiB memory budget, with at most 256 KiB per transaction; transactions that
exceed either memory allowance use temporary zstd frames with checksums. Runtime settings
`generation.pending_transaction_bytes` and
`generation.pending_transactions` bound the aggregate pending encoded bytes and
transaction count; they include completed transactions awaiting source order.
The byte budget counts uncompressed encoded facts, independently of spool compression.

Exceeding a limit fails rather than dropping events or publishing an incomplete
transaction. Frames from one source backend share segments, rotated at an 8 MiB
compressed target between transactions. A single frame may exceed the target.
Processing releases its frame's reference; a retired segment is deleted when no
pending transaction refers to it. A backend's active tail closes at source end,
so consumed tails do not accumulate across sessions. Failure diagnostics may retain
unprocessed scratch within the admitted budget. These limits do not cap Java
object overhead, auditing state, RowID storage, PG memory or total disk use.
Failure metadata streams at most 65,536 pending transaction details and explicitly
marks truncation; aggregate pending counts and bytes remain complete.

Optional `generation.operation_target` counts emitted workload Trace logical
records, including transaction-control records and index expansion.
`generation.output_byte_target` counts physical bytes of all canonical proto
files, including Baseline, excluding metadata and full-output side files.
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

JDBC `request_end` seals result consumption on exhaustion, explicit ResultSet
close, statement close or reuse. It retains the original request identity when
another statement has already executed. Auditing releases an older sealed request
only after result completion, executor cleanup and transaction association agree
and the backend has moved to another request label; ExecutorEnd itself precedes
scan cleanup. The latest label remains available for commit/rollback.
Sessions admit at most 65,536 pending requests and 1,048,576 buffered row-hash/detail
entries; overflow fails generation without publication. These entry limits are
not byte or whole-process memory caps. Connection close seals remaining results.

Full output additionally retains the complete-row Baseline, incremental Parquet,
raw capture and detailed `workload.ops`. The Parquet, Locator, independent row
oracle and relocation contracts below apply to full output. Proto-only output
supports RowIDs; it rejects Locator reference replay and the Parquet oracle.
Reference replay still checks every native expected count and generates ordered
result digests for service replay. This is not an independent source-row oracle.

Both strategies carry the Index Service protobuf request messages defined in
`src/main/proto/index_service.proto`, wrapped by `index_trace.proto` source
positions. A query group is one SearchGroup request with one position per member.
Commit counts describe unfiltered requests; replay reassigns request IDs, client
routing and transaction positions/counts when filtering or batching changes.
TraceBegin records retain source transaction boundaries and are not RPCs.
The source projection creates each payload once; full output adds detailed
operations as a side output of that projection. Service replay forwards stored
Write, Search and SearchGroup messages directly. Operation views support semantic
validation, filtering and the reference model, without regenerating the payloads.
Baseline batching wraps stored Writes; commit counts describe requests actually
sent after filtering.

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
identity hashes the ordered concatenation of each shard's 32-byte logical digest
and big-endian int64 operation count. No detailed-operation encoding is needed to
compute these identities. Detailed operations are diagnostic side output;
replay, locality analysis and the full-row oracle read the protobuf requests. Readers enforce file,
logical and inventory identities, transaction positions, commit counts and
complete source-member coverage.

## Two artifact layers

The generator observes requests, parameters, results, index candidates, row changes, and commits / rollbacks from an actual execution. Full output additionally persists these source facts as the raw layer; proto output does not. The canonical layer converts only declared supported index operations and retains their source-event associations. It does not rerun SQL to infer results. PG internals such as page splits, HOT, and VACUUM are not translated into downstream KV algorithms.

Before capture and Baseline export, the [source-scope](#source-scope) check verifies that actual PG tables, complete columns, and all primary / secondary indexes match the pinned source and configuration. Checks are read-only and do not create missing objects or repair the environment. Native runs save preflight results; this evidence is not fabricated for historical artifacts. Catalog support and canonical Trace support are accepted separately for each of the three sources.

The default native execution is HyBench `runHTAP`, capturing multiple AP / TP / concurrent AT-IQ sessions with row-change triggers covering all eight tables. **Canonical projection** processes source transactions by PostgreSQL's completion counters, including sequential workload phases and overlapping sessions (see [Identity and versions](#identity-and-versions)). It is schema-driven for all three sources. Each access unit, or BitmapAnd group, becomes ordered queries that reproduce native results under the [Access projection](#access-projection) rules; otherwise, the entire compilation fails with a reason. Index maintenance covers every primary and secondary index in the fixed schema, supporting indexed-column value UPDATEs, DELETE / reinsertion, reads after writes in committed transactions, and routing across clients.

Full output saves one zstd stream per backend, `pg-<pid>.jsonl.zst`, and
`jdbc-*.jsonl.zst` for JDBC clients. The generator's raw side output compresses the
same source facts used by the projection; it does not reread them to generate
requests. Explicit file-based raw audits require one JSON object per
line, reject invalid JSON, uncompressed files and unfinished zstd frames, and
require a completed capture manifest. Every PG stream starts with `capture_start`,
whose `detail` is `summary` or `rows` (default `summary`); JDBC streams start with
`connection`. `capture-run.json` binds stream identities, logical event digests
and audits. Full output also binds the retained compressed files' SHA-256 digests.
Proto output retains the source identity/audit snapshot without those raw files.
Instrumentation lives in `src/main/native/postgres/sibench_capture.c`; the patch
adds observation hooks and completion metadata in PG executor, index-access,
heap, process and transaction paths. Exact repeated comparisons use the published
Trace, including its recorded order among concurrent read-only transactions
tied at one completion counter. There is no production raw-to-Trace conversion command.

PG accumulates facts for each access unit and writes one `scan_summary` before the event that ends it: the next `scan_rescan`, `scan_end`, transaction abort, or connection close. The summary includes fetch-call counts, candidates found, whether the last fetch found a candidate, scan direction, rechecks, heap-fetch counts and visible rows, visibility-map all-visible rows, bitmap candidate and unjudged-candidate counts, whether bitmap filling finished, and whether row identity is missing. When a consumer stops a scan early, it also records visible rows before the last value, completed value segments for array keys, the last value, and the PKs of rows returned at that value. When the first member of a BitmapAnd round ends, it records whether any candidate produced by all inputs was never judged. Bitmap heap verdicts are aggregated by the set of inputs that produced each TID into `bitmap_verdicts` (visible / invisible counts and lossy-page counts). They are written before any input changes unit and are equivalent to per-TID attribution. `executor_run_end` records returned-row counts and a 64-bit FNV-1a hash for each row after column-text normalization. Client `consumed_row` uses the same algorithm, defined in `ResultRowHash`, for client / server result checks. With `detail = rows`, all per-row events are also written: `scan_next`, `heap_visibility`, `vm_visible`, `bitmap_candidate`, `bitmap_end`, `bitmap_heap_candidate`, `bitmap_heap_lossy`, and `result_row`; client rows retain column text. The ledger derives facts from those events and requires every summary to match exactly, validating summaries against the same native execution. Per-row events in a `summary` capture cause failure.

Other events are identical in both detail modes. `plan_scan` binds an index-scan descriptor to a plan node (index / index_only / bitmap). `plan_bitmap_heap` binds a bitmap heap-scan node to every index scan supplying its bitmap, recording the composition in `tree` as a scan identity or `{"and":[...]}` / `{"or":[...]}`. A flat input list cannot distinguish an intersection of unions from a union of scans. `scan_rescan` carries scan keys and the statement snapshot; array keys using `= ANY` include `array` elements and `array_type_oid`, while row comparisons are marked `unsupported_key`. `row_change` and `index_insert_attempt` record write facts. `transaction_commit` / `transaction_abort` carry the transaction completion position `csn`. `executor_end` is a cleanup lifecycle event that may be absent on connection close. Each event carries request ordinal `req`, numbered from the first `executor_start` outside any executor, and executor nesting `depth`, associating SPI / trigger queries with their outer request. `executor_start` also carries PG core query-jumbling `query_id`; the capture server starts with `compute_query_id = on`, without changing plans. Row identity consists of the scanned index-key column value and table PK read from a visible heap tuple. This is the only row identity shared by KV and PG; a heap TID has no KV counterpart. The ledger and compiler reject traces missing required fields. Files preserve session-local order; only completion positions establish order across sessions, as described in [Identity and versions](#identity-and-versions).

## Access ledger

The generator builds the ledger from the same source events in its projection pass and writes `access-ledger.json` in the Trace directory. The standalone diagnostic can read retained raw PG facts without connecting to PG. The standalone `access-ledger` command produces the same report, sorting inputs by filename and choosing the lexicographically first template display text. It can list all rejections when compilation fails. Each index rescan is an access unit, including every inner-table rescan in a nested loop. Each raw event belongs exactly once to an access unit, write maintenance (`index_insert_attempt`), executor lifecycle, transaction boundary, result row, row change, index build, or plan binding. Category totals must equal the input event count. Candidate entries, visible rows, and SQL result rows describe three properties of the same access and are not added together. Bitmap candidates are TIDs before visibility checks. Visible rows are index rows visible through heap checks or the visibility map, or bitmap candidates judged visible by the heap. A candidate TID's verdict contributes to every index scan that produced it, so each side of BitmapOr gets its own visible-match count. BitmapAnd's heap checks only the intersection; inputs executed in the same round form one group with one visible count. PG skips later inputs once an intersection is empty, so a round includes only inputs actually executed. Per-TID or aggregated heap verdict events are reported separately as `bitmap_heap_verdicts` and are not counted again as access-unit events. `scan_summary` belongs to its access unit; `capture_start` is reported separately as `capture_header`. Each input file reports its `detail`.

Each access unit carries the same projection decision used by the compiler, described below. `projection` summarizes projection shapes, rejection reasons, and exclusion reasons; any rejection sets status to `has_rejected_units`. Template identity is PG `query_id`, emitted as a string to avoid int64 precision loss in JSON tools. SQL with literals removed is display text only; multiple texts for one query_id are marked `text_variants`. Each template lists plan sequences (request-bound plan node, scan kind, and index) and request counts, with `plan_variants > 1` when several exist. It then reports access-path units, candidates, and visible rows by (source, plan node, index, scan kind, decision). Zero access units is an observation, for example a request using a sequential scan, rather than an inference from a plan.

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

Remaining rejections correspond to **missing facts** (TIDs absent on lossy pages, unjudged candidates after an early-stopped heap scan, or all-visible index-only rows without PKs) or key shapes outside the fixed source scope. No rejection remains solely because available facts cannot be expressed by the model.

Keys are encoded according to the indexed column type. Scan keys must belong to that column's btree operator family: int2/4/8 for int, timestamp and date for timestamp, text/varchar for varchar/text, and bpchar for CHAR. Source schema preflight requires text columns and indexes to use the database's default collation, so `lc_collate` determines whether text intervals preserve order. Native capture databases for all three sources use C collation. Projection describes the index access itself. Templates whose aggregates, sorting, DISTINCT, or LIMIT make returned counts differ from visible counts retain both. Query expectations use visible counts rather than returned counts.

## Validation responsibilities

`RowStateAudit` validates captured row transitions and final source state. Starting from the exported Baseline, it compares complete before / after images according to schema types, advances state for committed changes, and excludes rolled-back changes. After capture ends and the business database is quiescent, it reconciles against PG's final state across all tables. Single-session audits strictly preserve source-operation order. Multi-session audits connect row causal chains while preserving each session's order for a key. The audit establishes row state only; the compiler derives global commit order and query snapshots from completion positions. Rolled-back images are checked for row shape, types, and primary-key stability without joining the committed change chain.

The audit writes `row-state-audit.json` in the raw directory. Report digests bind the schema, raw inputs, Baseline identity, and audited changes. `changes_sha256` is computed while checking changes in deterministic table / key / causal order, including each change's source session and position; no duplicate transition file is written. Repeated calls with the same report path recheck input digests without querying PG or checking row chains again; changed inputs and reports referring to persisted transition files are rejected. The report is published atomically last. Failed remnants do not establish success; retry with a fresh report path. A new audit requires the final database state from the original capture or an exactly restored copy.

Production generation checks source/schema identity, baseline file integrity, native session/request completion, transaction and snapshot semantics, and source access coverage. The baseline's `baseline-manifest.json` binds its schema, planned row counts, file inventory and SHA-256 identities; the Trace binds that manifest through `baseline_manifest_sha256`. An export manifest establishes file identity and coverage, not an independent comparison with database values. Compilation builds the access ledger and, when source counter snapshots are supplied, refuses mismatches before publishing a complete Trace. The normal generation entry point always supplies these snapshots.

The export entry point checks the actual source schema. Its snapshot planning
count enforces YCSB's declared load size before exporting rows, then the emitted
row count must match that plan. Native loading does not perform a separate full
count for its report. Proto publication reuses a successfully closed writer's
physical digest; projection and replay verify stored bytes during their necessary
reads. Source access and transaction validation remain in the common projection;
generation does not reread raw files to compute change-type statistics.

Independent acceptance is explicit. `audit-baseline` compares all source-column values with the unchanged initial database, checks generated RowID/timestamp invariants, and writes `verified-manifest.json`. In the single-database native workflow this audit must run before any workload changes; the explicit `--audit-baseline` capture option provides that boundary. `audit-rows` compares captured changes with the final database. Generation records `verification.mode=runtime`; independent row-audit reports belong to the separate acceptance workflow and are not inputs to the generator. Runtime verification does not claim independent row-state verification. Independent reports are not prerequisites for normal generation, and no cross-run certification substitutes for this run's runtime checks.

Projection acceptance checks source-event mapping, Parquet content and complete index expansion. The independent Parquet oracle checks current/historical keys, versions and values. Reference replay generates a per-query digest binding query identity, result order and complete `(PK, version, value)` results; service replay checks each query against it. Native counts remain independent source facts, and expected digests must not be derived from service responses. Changing client count, dispatch settings or backend does not require repeating an unchanged source audit, but input identity and actual service results remain checked.

PK value UPDATEs are outside the supported workloads for all three sources; both committed and rolled-back attempts are rejected. Ordinary-column / secondary-index-column UPDATEs with an unchanged PK and DELETE / reinsertion remain supported. CHAR ignores trailing ASCII spaces only; VARCHAR / TEXT preserve their text. REAL is compared at the source's single precision, and date / timestamp values are compared according to their types.

## Identity and versions

```text
PrimaryKey   = (Table, IndexID=0, PK, CommitVersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, CommitVersionDescending)
Locator      = (FileID, FileRowPosition)
LogicalValue = RowID
```

- IndexID=0 identifies the primary index. TableIDs start at 1 in source-schema table order: the fixed eight-table order for HyBench, sbtest1..N for sysbench, and usertable for YCSB. Secondary IndexIDs follow each table's source `CREATE INDEX` order. `IndexCatalog` is generated from the source schema. The manifest records source provenance (benchmark, source directory and commit, schema-file SHA256, and YCSB / sysbench native configuration) and complete definitions. Preflight reconstructs and checks them individually, rejecting self-declared partial inventories. transfer indexes 1/2/3 are sourceid/targetid/type respectively. Nonunique secondary predicates remain prefix queries rather than a single PK Get.
- PK and IndexedValue are self-delimiting, order-preserving `IndexKey` components. Unsigned byte order matches PG btree's default ascending, NULLS LAST order, with text restricted to C collation. Concatenated components compare as tuples. NULL is `0x02`; non-NULL components start with `0x01`. int2/int4/int8 use a sign-bit-flipped big-endian int64. Timestamps encode microseconds since 1970 the same way, with -infinity / infinity at the extremes; BC dates and submicrosecond precision are rejected. Text is UTF-8 terminated by `0x00`, since PG text cannot contain NUL. CHAR first trims trailing spaces to match bpchar comparisons. The PG differential test `IndexKeyOrderIT` checks ordering of int8, timestamp, C-collated varchar / char, and NULL.
- KV byte layout: `Table(uint32 big-endian) ++ IndexID(uint32 big-endian) ++ [IndexedValue] ++ PK ++ (~CommitVersion big-endian)`. A point lookup for v scans `[v, succ(v))`. An inclusive lower bound uses `v`, an exclusive lower bound uses `succ(v)`, an inclusive upper bound uses `succ(v)`, and an exclusive upper bound uses `v` as the exclusive endpoint. `succ` is `IndexKey.prefixSuccessor` and works for any following PK and version bytes.
- SIBench increments CommitVersion in original session order for every committed transaction that executed a request, including read-only commits. Baseline=0, and all changes within a transaction share its version. Transactions committed for connection setup and capture request labels (`SET application_name`) execute no requests, receive no version, and are reported as `utility_only_transactions`. Each YCSB autocommit request therefore advances exactly one version. Business timestamps, wall clocks, XID magnitude, and downstream completion order are not used.
- Sequential multi-session capture requires `session_order=sequential` in `capture-run.json`. Each native workload runs in its own client process; the next starts only after that process exits and no capture backend remains. Sessions are concatenated in workload order with continuous versions and work units. This order comes from orchestration facts, not PID or filename inference.
- Sessions with `session_order=concurrent` are merged by PostgreSQL's transaction completion counter, `ShmemVariableCache->xactCompletionCount`. Transactions holding an XID, including rolled-back write transactions, increment the counter in `ProcArrayEndTransactionInternal` under the exclusive `ProcArrayLock`, exactly when they become visible to other snapshots. At that same point, the patch stores the incremented value in the transaction's `PGPROC`. Group cleanup is performed by the leader, so the value is stored in the member's `PGPROC` rather than a backend variable. Commit / rollback records emit this exact value. Thus, a commit is visible to a snapshot exactly when the snapshot counter is at least the completion position, and write transactions have distinct positions. Read-only transactions without an XID change no snapshot and do not increment the counter. They record the counter observed at commit and sort after the write transaction at the same position. Transaction-sized fragments are merged k ways by (position, writes first, session order), without inference from PID, XID magnitude, file order, or wall time. Within a session, write positions must strictly increase and read-only positions must not decrease. Two writes sharing a position cause failure. Scans cannot cross transaction-sized fragments; access partitioning restarts for each fragment so that reused scan pointers from different backends cannot collide.
- Rollback does not advance CommitVersion, allocate a RowID, change committed index state, or write incremental Parquet. Later transactions continue from the latest committed state.
- transactionId is the ordinal of the source commit event. A change's mutationOrdinal is its row-change ordinal within the transaction, shared by secondary expansions. Queries in the group carry the same transactionId, with mutationOrdinal indicating how many row changes preceded the query. Both fields support source association and order checks; neither is a KV version suffix. The service adapter derives execution positions from the sent operations, and the service tracks transaction ordering and complete-commit publication as defined in the [Index Service contract](index-service.md#data-transactions-and-reads).
- ReadTimestamp represents the query's original statement snapshot, expressed as a visible-commit upper bound. At T, select the largest version `<=T` for a key. An absent version or a selected deleted version means the entry does not exist; readers must not skip tombstones to return older values.
- **Transaction-local reads** preserve the original statement snapshot and carry the source transaction's CommitVersion (SEARCH.version) and mutationOrdinal, with TRANSACTION_LOCAL_SEARCH set. Results start from the latest snapshot state `<= ReadTimestamp`, overlaid only with that transaction's writes at this version before mutationOrdinal. Other transactions committing between these positions remain invisible. Send in original position order: earlier changes → query → later changes. After the transaction ends, only the final effect for each key and version remains. Readers require `version>0` and `readTimestamp<version` for transaction-local reads.
- In sequential sessions, queries before a transaction's first change use the previous commit's ReadTimestamp. Concurrent queries use the last writing commit visible to their statement snapshot's counter. Read-only commits do not change data and are not read points. Each scan event records the snapshot counter, xmin, xmax, and xip. The compiler selects the last writing commit whose position is at most the snapshot counter as ReadTimestamp; it may precede the commit immediately before the query's send position, and SNAPSHOT_SEARCH is set. For each scan, the compiler cross-checks that commits considered visible by the counter do not appear in that snapshot's own xip. A conflict means one ReadTimestamp cannot express the visible set, causing the entire compilation to fail rather than approximate it. Snapshots marked `suboverflowed` cannot cross-check individual subtransactions; those checks are omitted and reported separately. Raw xmin/xmax/xip retain their visibility meaning and are not used directly as commit versions. Explicit historical queries select available historical points; future read points are rejected.
- UPDATE preserves RowID; reinsertion after DELETE allocates a new RowID. Rewrite changes neither the logical version nor RowID.
- FileIDs start at 1 and are never reused; positions within a file start at 0. Datasets are isolated, and int64 values are parsed without loss.

## Work units and routing

`workUnitId` identifies a request work unit independently of index keys, transactionId, CommitVersion, PID, and physical node identity. The compiler treats one audited top-level source SQL execution, identified by a JDBC / PG pair or server request ordinal, as one work unit. It assigns IDs from 1 at `executor_start` in source-request order before rollback or secondary-index filtering. Scans, nested queries, row changes, and their primary / secondary expansions within the same request share the identity. A request is not artificially split into multiple work units.

`work-units.jsonl.zst` records `work_unit_id`, original `application`, `pid`, session-local request ordinal `request`, and the request-start `source_event`, numbered continuously across sessions, in ID order. Its SHA256 is stored in the manifest; binary operations retain `sourceEvent` and `workUnitId`. Requests without effective index operations remain in the identity inventory but do not count toward runtime sends. A SQL work unit is an explicit approximation of an execution task, not a reconstruction of a real serverless task DAG or data-node topology.

Replay uses the fixed rule `clientId = (workUnitId - 1) % client_count`. It does not store fixed clientIds, hash index keys, bind source transactions to clients, or offer alternative routing strategies. One work unit always uses one client, while multiple work units in a transaction may span clients. Changing client count changes sending connections only, leaving Trace, versions, Parquet, and operation order intact. Baseline assigns one load work unit per 4096 consecutive rows, with each row's index expansions sharing its identity. Load identities and runtime SQL identities are counted separately; Baseline coverage cannot replace runtime coverage.

`client_count` defaults to 1; 64 and 128 are experiment settings, not hard limits. After secondary-index filtering, preflight checks that actual operations cover every configured client rather than merely counting distinct work units. Insufficient coverage is rejected before actual loading / sending. Extend native capture instead of repeating Trace or fabricating units to fill clients. Reports include configured / used clients and per-client operation, query, write, and work-unit counts. Coverage does not establish load balance or parallel connection throughput.

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

Non-SEARCH operations have readTimestamp=-1. `IndexMutation` explicitly expands each row into the primary index and every secondary index of its table. The sender must not maintain them implicitly again. The write interface offers no implicit transfer-only expansion.

Canonical Trace preserves the original relative order of queries and changes within committed source transactions. Queries occupy the native scan-start position, with candidate counts filled in at scan end. Each change writes the primary index first, then expands secondary indexes in increasing IndexID order. An indexed-value change writes LOGICAL_DELETE for the old `(Table, IndexID, IndexedValue, PK)` at the same commit version, followed by UPSERT for the new identity. Secondary entries whose keys are unchanged also receive UPSERT to refresh the row version and Locator / RowID. This neither changes index definitions nor deletes business rows or physically removes historical versions.

Baseline emits INSERT per index; runtime INSERT emits UPSERT per index; DELETE invalidates the primary and all secondary entries. Reinserting the same PK allocates a new RowID; UPDATE retains the original RowID. Old entries, new entries, and the primary entry share source event, version, transaction, mutationOrdinal, workUnitId, and RowID. Value locations agree except that deleted entries have no physical location. `expansionPosition` is consecutive from 0, and `expansionSize` is the complete expansion width. Queries and control records cannot interrupt an expansion. Query groups reuse these fields for group position and width and likewise cannot be interrupted. Before secondary-index filtering, preflight checks the full inventory, complete expansions, and source work-unit bindings, rejecting truncation or missing members.

During full generation, incremental files are created lazily for actual tables as `<table>-versions.parquet`, with initial FileID equal to the maximum Baseline FileID plus TableID. Each write retains `_sib_timestamp`, `_sib_mutation_ordinal`, and `_sib_row_id`. The same key and version overwrite in order. Intermediate effects serve original transaction-local queries without creating additional commit versions; later source transactions see only the final committed effect. Primary-key value UPDATE is explicitly rejected as out of scope. Incremental files roll after a closed row group reaches the source configuration's `data_layout` target size, independently of replay-backend SST settings; see [Data files](#data-files). The first FileID is the Baseline maximum plus TableID. Later FileIDs increase globally beyond the initial-file range reserved for all tables, using full int64 values. Actual row counts and SHA-256 are checked per file, and Locators point to actual files and positions.

Trace uses a compiler and reader built from the same commit.

## Delivery selection

Only committed changes are sent downstream by default; this convention is not limited to a particular Index Service scenario. Raw Trace preserves change attempts, commits, and rollbacks. The source projection identifies transaction outcomes and expands only committed business changes into downstream index updates. Rolled-back changes are not published, do not generate compensating reverse operations, and have no rollback switch. Normal business rollback does not imply capture failure. Unfinished transactions must not be silently consumed as rollbacks.

Committed-only delivery constrains writes while retaining queries. In successfully committed transactions, reads after writes are converted using the timestamps and transaction positions above. In rolled-back transactions, supported queries before the first change remain; queries after it are excluded because their read environment depends on excluded changes. Filtering conservatively asks whether any change has already occurred, without inferring dependencies between query keys and changed keys. Exclusions are reported separately.

The compiler emits a transaction's canonical operation group and incremental rows only after seeing its source commit. An abort that passed paired auditing excludes changes and subsequent queries. Canonical Trace emits neither ABORT nor reverse operations; full output retains rollback facts in its raw side output. Manifest fields `change_delivery=committed_only`, `raw_row_changes`, `discarded_aborted_row_changes`, `logical_row_version_writes`, and `index_version_writes` describe the delivery convention, raw row changes, rollback exclusions, committed row changes, and expanded index writes respectively. It also reports `transaction_local_searches` and `discarded_aborted_transaction_local_searches`. `aborted_transactions` includes explicit rollbacks and aborts on connection close. PG `index_insert_attempt` is not a downstream write.

The following driver settings live in `config/sibench.yaml`, sharing the file with `postgres` and `analysis`:

```yaml
target:
  secondary_indexes:
    enabled: true
replay:
  strategy: single_pass
  dispatch_mode: operation
  client_count: 64
  max_in_flight: 4096
  request_timeout_seconds: 600
```

The secondary-index switch filters every load, query, write, logical invalidation, and relocation with IndexID>0. It leaves primary-key identity, source order, versions, dependencies, and Parquet intact and does not replace filtered queries. Query groups are sent or filtered as a whole: if any member uses a secondary index and secondary indexes are disabled, the entire group is filtered, never leaving a partial intersection.

| dispatch_mode | Driver input unit | Sending contract |
| --- | --- | --- |
| operation (default) | One index operation. | Route and send each operation locally after validating its complete source group. |
| transaction | An ordered operation list, including queries, from one committed source transaction. | Organize input by source group, still routing and sending each member locally; a group may span clients. |

Both modes send the exact same operation sequence without merging same-key changes, moving queries, or reordering across transactions. `transaction` does not assign one client or one RPC per group. For service replay, both modes use versioned operations and the complete-commit publication protocol; neither opens a downstream SQL transaction or sends rollback operations. Group statistics belong to the driver and do not count adapter calls.

A single global sending thread invokes clients in Trace order, waiting for the current operation's `sent` acknowledgment before sending the next. `ReplayTarget.SendHandle.sent` marks completion of the local transport boundary, not merely enqueueing before serialization. gRPC uses `ClientStreamTracer.outboundMessageSent` to confirm serialization and handoff to the local transport, without claiming socket-send completion or remote receipt. Incomplete Baseline batches and query groups buffer members in the client. The last member triggers the actual RPC and waits for that transport boundary. Logical members and RPCs are counted separately. `response` means the adapter has associated and checked the service response; errors or mismatched results complete it exceptionally. Responses may complete out of order. Remote arrival, execution, and visibility order across connections are outside this local sending contract. Index Service handles cross-client semantics; the benchmark adds no visibility barriers, reverse operations, or automatic retries.

The global `max_in_flight` limits outstanding responses, defaulting to 4096. At the limit, sending waits for responses to release capacity; completion drains outstanding responses. Timeouts start before send and cover local sending and response waiting. `request_timeout_seconds` allows 1–86400; when omitted, it uses 60 seconds. The public configuration sets 600 seconds for full source inputs. Adapter send calls must return handles promptly without unbounded blocking. The adapter implements actual transport timeouts and resource cancellation on close. Failures stop further sends, close opened clients, and cancel outstanding futures without automatic resumption. Runtime execution starts only after initial loading completes and drains. This is a loading boundary, not a runtime visibility barrier across clients.

The driver buffers and validates a complete source group, checking the closing COMMIT before sending. The service receives versioned writes and queries carrying ReadTimestamp, followed by a complete-commit marker with the actual operation and write counts. Trace BEGIN/COMMIT records are not forwarded verbatim; the adapter constructs the marker described in the [Index Service contract](index-service.md#data-transactions-and-reads), including for transactions made empty by filtering. Each group is limited to 1,000,000 expanded operations, including filtered operations. The compiler also bounds pending source operations. These are input protections, not a large-scale spillable-state design. After concurrent sessions are merged by PostgreSQL's transaction completion counter, sends follow commit order. Queries carry their snapshot's ReadTimestamp, which may precede the previous sent commit (SNAPSHOT_SEARCH).

Reports distinguish persisted records (including controls), source index operations, secondary-index filtering, actual sends, source transactions, and operation kinds. In both modes, `target_calls` counts logical adapter calls, including service commit markers, rather than network requests; `commit_markers` reports those markers separately. `source_dispatch_groups` counts nonempty source groups in transaction mode only. Reports also include `operation_order_checksum`, independent of client count, completed / pending responses, peak in-flight requests, and backpressure wait time. `sending_threads=1`, `service_visibility_owner=index_service`, and `downstream_transaction_api_required=false` make responsibilities explicit. The reference adapter executes immediately against shared model state, validating routing and operations / results without simulating network-concurrent visibility. Point and range queries select versions at their read timestamps. RowID relocation emits no index writes. Multi-sender replay and physical version GC are not implemented.

## Completion and failure

Derived Baseline includes `resolved-config.json`, `files.jsonl`, `manifest.json`, and Parquet, retaining `INCOMPLETE` until finished. Canonical compilation likewise publishes a completion manifest and checksums. Consumers reject incomplete or modified artifacts.

Native execution writes `capture-run.json` in the source-control directory selected by `capture.raw_directory`:

- `running`: incomplete and cannot be published.
- `failed`: retains the first error, SQLSTATE, request / transaction context, and stack trace. It cannot be published or resumed.
- `complete`: connections closed normally, and correlated same-execution audits
  of requests, results and transaction boundaries passed. `sessions` binds stream
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

Diagnose unexpected errors, fix their causes, restore a known initial state, and rerun in a fresh directory. Do not guess PG internals or fabricate events. Raw auditing checks request identities for explicit business rollbacks and aborts on close. A transaction still open at close requires a corresponding server end boundary; closing a file cannot silently discard it. The eight-table state audit and compilation within the supported scope exclude rolled-back changes without rejecting an otherwise valid capture or automatically retrying. Compilation still rejects mismatched transaction XIDs, unfinished scans, missing completion positions, and snapshot conflicts. Sending still rejects unmatched boundaries, incorrect version / query positions, incomplete expansions, and thread switches. Correct final state cannot replace proof of global event completeness.

## Long-running inputs and validation

Generate a finite native workload into a persistent Trace, then use `single_pass`
for repeatable independent service tests. Generation projects committed transactions
as facts arrive and releases processed scratch. Pending-fact and audit-entry
limits fail on overflow. Optional generation operation/output targets request
cooperative native admission stop and drain completed transactions as described
above. Resumable segments and checkpoints are not implemented. Native SQL,
transaction boundaries and configured concurrency are preserved.

Budgets count effective runtime operations after secondary-index filtering, identically for both dispatch modes, excluding Baseline, controls, and derived probes: warmup operations + ceil(rate × measurement seconds × (1 + margin)). Insufficient supply is rejected before loading / sending. Preflight establishes input quantity, not achievable throughput or sustained scheduling.

`check-replay` validates the manifest, digests, pinned source-index inventory, complete expansions, source work-unit bindings, format, record counts, CRC, filtered client coverage, and budgets without invoking an actual adapter. Replay and locality analysis require a complete manifest with nonnegative int64 `baseline_rows`, `source_work_units`, `baseline_operations`, and `workload_operations`; missing or invalid coverage counts are rejected before loading. `replay` selects its value mode from `target.service.value`, or `target.value` in a standalone reference configuration; if unspecified, both value modes are checked. Production replay requires directory-level preflight.

Ordinary reference replay checks every native expected count and publishes full
ordered query digests. The whole final primary-state checksum is an additional
scan used by explicit `validate-replay-matrix` state-equivalence diagnostics;
ordinary reference reports do not compute it.

`validate-snapshots` requires canonical Trace with source-code and Baseline provenance and no relocation. The oracle independently reads Baseline and incremental Parquet rather than deriving expectations from secondary Trace values. It checks every modified PK and complete secondary identity for key/version/Locator/RowID at Baseline, before and at every commit affecting that key, and at the final point. Each source secondary index gets up to 64 deterministic equality predicates: half from modified values, the rest uniformly distributed across the Baseline value domain including endpoints. Limited range probes for the primary and every source secondary index use 9 quantile boundaries, checking adjacent lower / upper bounds as inclusive and exclusive from each side in ascending and descending order, plus unbounded scans in both directions and ranges anchored on modified values. Predicates and intervals check result key/version/value and order at Baseline, before and at the first relevant change, and at the final point. Source-file digests, incremental positions, RowIDs, versions, and transaction-local mutation positions are also checked. Query groups are covered only through member lookups. This sampling does not prove all predicates. Probes are reported separately and neither written into Trace nor counted as native workload.

## Source scope

Only pinned, clean benchmark submodules and their declared source layouts are accepted. Preflight is a read-only point-in-time check. It neither creates missing indexes nor detects external DDL drift throughout execution.

| Source | Tables and indexes | Native execution scope |
| --- | --- | --- |
| HyBench | Eight tables, 8 single-column primary keys, and 13 ordinary single-column secondary indexes from the fixed DDL. | runHTAP AP / TP / XP; freshness DELETE is not called by default. |
| YCSB JDBC | VARCHAR primary key and the configured number of TEXT fields; no secondary indexes. | Native A / B / C / F / D / E; E uses readallfields=false. |
| sysbench PG | Configured table count, each with an id primary key and k secondary index; auto-inc can be enabled or disabled. | Six oltp scripts, including DELETE / reinsertion; standard scripts do not query by k. |

The actual table inventory, complete column order / types / widths / nullability / defaults, and primary / secondary index definitions must match the source. Sequence-default references and ownership must also match. Extra tables, views, temporary tables, partitioning / inheritance, RLS, generated columns, extra unique constraints, composite / expression / partial / INCLUDE indexes, custom operator classes / collations / ordering, and non-B-tree access methods are rejected. Ordinary logged and unlogged tables are supported by schema validation; native generation requires unlogged source tables. The configured database, role, version, port, isolation, and parallelism settings must match the actual connection. Text intervals also have the C/POSIX collation restriction above.

Primary-key value UPDATEs, including rolled-back attempts, are out of scope. Ordinary-column / secondary-index-column UPDATEs and DELETE / reinsertion with the same PK are supported. Schema checks do not establish row counts, current sequence values, or native-load correctness.

## Data files

- Baseline and incremental data share the same Parquet byte policy, configured under `data_layout` for native inputs and `data_layout.parquet` for derived inputs. `target_file_size_bytes` is a positive integer, with examples defaulting to 512 MiB. `file_size_max_overshoot_percent` defaults to 5 and accepts integers from 1–100. Complete closed files include compressed pages, page indexes, and footers. Normal files range from the target to `target + floor(target × percent / 100)`.
- Each Table / Partition / Bucket write stream allows at most one below-target EOF tail file, which must still respect the upper bound. Task sharding creates no extra tails. At most 32,767 row groups are allowed. Oversized rows, unsatisfiable group / page budgets, and metadata limits fail explicitly without padding, splitting rows, or relaxing overshoot.
- Maintained templates use parquet-java, Zstd level 3, single-threaded compression, 1 MiB pages / dictionaries, and enabled dictionaries. Supported codecs are zstd / snappy / none, with level=0 for the latter two. Buffer budgets are not hard JVM RSS limits, and the default free-disk stop threshold is not a capacity guarantee.
- Native Baseline assigns consecutive RowIDs in table / primary-key order within one exported snapshot. FileID is `table ordinal (starting at 1) + file ordinal (starting at 0) × table count`, and manifests are independent of task scheduling. Derived generation follows fixed source order and configured bucketing without claiming byte-for-byte identity with native random data.
- Changing the target or layout requires fresh output and corresponding Locator / Physical Trace. Replay does not recut Parquet. Native generation exports a fresh Baseline for each run and rejects existing output directories instead of reusing another run's files.
- `versions.gc.owner` currently supports only `none`. Business DELETE publishes versioned invalidation; physically deleting the latest entry must not resurrect older values. Disabling historical queries or filtering secondary indexes does not reclaim historical versions.
- `check-relocation` moves only the latest live rows of tables actually modified: the exact-version Locator changes, and RowID stays the same. Currently, each table allows one output file and rejects workloads requiring rollover. It does not handle protected older versions, automatic triggering, old-file deletion, or the complete Rewrite / GC lifecycle.

Backend SST / WAL / MANIFEST files are index state controlled by the engine. The data-file target above is not an SST size setting. See the [Index Service contract](index-service.md) for service acknowledgment and durability levels.
