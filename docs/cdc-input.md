# Reusable PostgreSQL CDC input

`prepare-cdc` prepares row changes for repeated Pixels experiments. It runs the selected HyBench TP/AT workload against one fresh PostgreSQL database and captures real committed changes using the pinned pixels-sink converter and ProtoWriter. It retains the original baseline CSVs and closed CDC files. It does not create an Index Trace or start Retina, a Pixels metadata service, etcd, Trino or freshness queries.

## Commands and execution

Build the verified Pixels generation package and HyBench client as described in the root README. Preparation uses the package's source-built PostgreSQL runtime and Java sink binding on the generator machine; `pixels_host` is not contacted. Use a fresh `artifacts.directory` under `results/capture` in the selected HyBench profile:

```bash
java -jar build/sibench/sibench.jar prepare-cdc config/sibench.yaml
java -jar build/sibench/sibench.jar check-cdc-input results/capture/hybench
```

The command selects exactly one HyBench/cdc/Pixels entry from the runtime configuration. Other benchmark entries are validated as selections but their engines are not built, opened or executed. Unsupported or missing CDC selection, an existing output directory, source/build mismatch and invalid configuration fail before capture starts.

Preparation runs TP once and XP once using the profile's phase durations and transaction-client counts. AP/IQ clients are disabled. `trace.workload_requests` is an Index Trace operation budget and does **not** stop CDC preparation; elapsed source workload phases define this input. The manifest records this distinction and the actual source transaction/row-change counts. Source read-only transactions and rollbacks produce no CDC transaction; native SQL reads remain in the workload. Source SQL, indexes and transaction boundaries are preserved.

The PostgreSQL baseline and converter schema are derived from the same identity-bound CSVs. Schema inspection validates the Pixels LOAD representation and widths without materializing another copy of the baseline. The converter receives this schema directly in a private registry; no external metadata lookup is required for the declared tables. Source primary-key changes are rejected, REPLICA IDENTITY FULL is required, and the pre-created pgoutput slot starts after baseline loading with no initial CDC snapshot.

Setup connection and native client execution have separate bounds. Each phase ends with a committed marker transaction; the controller waits up to two minutes for that transaction to reach the file writer, proving the preceding source prefix has been captured. It then requests normal source/decoder/writer shutdown, bounded to 60 seconds. Capture failure, a missing marker, incomplete transaction, failed writer, unexpected source-engine completion or failed shutdown prevents publication and retains diagnostics. A failed publication retains the closed source database. After publication succeeds, its database data directory is discarded.

## Retained input

The published input root contains:

- `manifest.json`: the final input catalog, published after normal capture close and validation.
- `native/Data_<sf>/`: the original baseline CSVs and the native client's bound runtime inputs.
- `schema.sql` and `pixels-schema.tsv`: the source DDL and ordered Pixels type/primary-key specification.
- `cdc/00000000.proto`, subsequent numbered files and `cdc/files.json`: whole-transaction row changes and the native writer's closed-file receipt.
- Effective configuration, build identities, client summaries, phase boundaries and generation diagnostics.

The final manifest owns the reusable input inventory. It binds baseline row counts and files, schema/DDL, the ordered CDC file list, physical sizes/SHA-256 hashes, transaction counts and source/client/build/configuration evidence. `cdc/files.json` is a writer receipt, not an independently published input. Input files are relative to their root, so the directory can be moved or copied without retaining the preparation database or original absolute paths. `check-cdc-input` verifies the published catalog, baseline content/counts, file framing, schema bindings, transaction/row completeness and hashes; symbolic links, path aliases/traversal, fractional counts, missing/unlisted/reordered files and unsupported operations fail.

The engine's sink-properties overrides may set `sink.proto.maxRecords` to a positive 32-bit integer. During preparation it limits complete transactions per file; it never splits a transaction. Its default belongs to the pinned sink configuration.

Each file contains complete transactions. Each frame is a big-endian signed 32-bit key, a big-endian signed 32-bit payload length, then a protobuf message from the pinned Pixels `sink.proto`. Key `-1` identifies TransactionMetadata; other keys identify RowRecord. Individual frames are limited to 16 MiB. Transaction frames appear as BEGIN, globally ordered rows, END. Global/table row positions and exact END counts must agree. UPDATE/DELETE carry complete before images; INSERT/UPDATE carry after images.

Row-frame table IDs are capture-local ordinals in `pixels-schema.tsv`, starting at 1. They are not physical table IDs of a later Pixels deployment. Source database/schema/table names remain in RowRecord; a target integration must bind those names and types to its fresh metadata. Source XIDs and source event timestamps are retained as source information, not as Index Trace versions or replay-wall-clock freshness measurements.

`counts.transactions` includes the marker transactions. `counts.changes` includes all row changes, `counts.businessChanges` excludes marker rows, and `counts.markers` counts the one-row marker transactions. They are distinct from distinct business rows, Index Trace operations and service RPCs. The marker report acknowledges file-writer acceptance; normal file close and final validation establish input publication. Neither signal measures target visibility or remote durability.

## Current boundary

The [Pixels experiment consumer](pixels-experiment.md) loads the bound baseline, rebinds source names to target metadata, and applies all files once or a complete transaction prefix. Trace export, business queries and sink freshness are separate experiment settings. Existing `generate` remains the live PostgreSQL → sink → Pixels Index Trace workflow; ordinary isolated index replay consumes Index Trace alone. Input preparation does not establish index speedup, visibility latency, SF1000 capacity or long-duration performance.
