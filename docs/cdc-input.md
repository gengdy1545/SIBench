# Reusable CSV and PostgreSQL CDC inputs

`prepare-csv CONFIG OUTPUT_DIRECTORY` generates a reusable HyBench CSV input without starting a database. `prepare-cdc CONFIG CSV_INPUT_DIRECTORY` consumes that published input and prepares row changes for repeated Pixels experiments. It runs the selected HyBench TP/AT workload against one fresh PostgreSQL database and captures real committed changes using the pinned pixels-sink converter and ProtoWriter. It retains the original baseline CSVs and closed CDC files. It does not create an Index Trace or start Retina, a Pixels metadata service, etcd, Trino or freshness queries.

## Commands and execution

Build the HyBench client as described in the [README](../README.md#build). CDC preparation also requires the verified Pixels package's source-built PostgreSQL and sink binding on the generator machine; `pixels_host` is not contacted. Use a fresh `artifacts.directory` under `results/capture` in the selected HyBench profile:

```bash
java -jar build/sibench/sibench.jar prepare-csv config/sibench.yaml results/capture/hybench-csv
java -jar build/sibench/sibench.jar check-csv-input results/capture/hybench-csv
java -jar build/sibench/sibench.jar prepare-cdc config/sibench.yaml results/capture/hybench-csv
java -jar build/sibench/sibench.jar check-cdc-input results/capture/hybench
```

Both preparation commands use the selected HyBench benchmark profile, independently of its native capture engine. Other benchmark entries are validated as selections but their engines are not built, opened or executed. CSV preparation verifies the source/client build only and reads no engine configuration. CDC preparation reads only the source PostgreSQL and sink settings from `engines.pixels`, and verifies the source binaries, converter JARs and their build inputs. It does not require the Pixels server, Retina library, etcd, target storage configuration or target property files on the capture host. The source bundle still comes from the pinned Pixels build; it is distinct from the instrumented native PostgreSQL build. CSV output must be fresh and below `results/capture`; CDC output remains the benchmark profile's `artifacts.directory`. A missing HyBench selection, an existing output directory, source/build mismatch and invalid consumed configuration fail before capture starts.

The CSV manifest binds the original CSVs, client runtime lists, source DDL, derived Pixels schema, generator parameters and build identities. `check-csv-input` checks contents, row counts and the derived schema. Capture rejects a different source revision, schema, scale or generator parameters before opening PostgreSQL. Client counts and phase durations may vary independently of the data. Multiple independent consumers may use the same published input; they must never modify it.

CDC output retains an independently usable copy of its baseline. On the same filesystem these immutable payloads use hard links; across filesystems they are copied. Removing a consumer output does not remove the canonical CSV input. The local `csv-input.json` binding and published source evidence retain the original CSV manifest digest. Published inputs must remain immutable, including their hard-linked copies.

Preparation runs TP once and XP once using the profile's phase durations and transaction-client counts. AP/IQ clients are disabled. `trace.workload_requests` is an Index Trace operation budget and does **not** stop CDC preparation; elapsed source workload phases define this input. The manifest records this distinction and the actual source transaction/row-change counts. Source read-only transactions and rollbacks produce no CDC transaction; native SQL reads remain in the workload. Source SQL, indexes and transaction boundaries are preserved.

The PostgreSQL baseline and converter schema are derived from the same identity-bound CSVs. Schema inspection validates the Pixels LOAD representation and widths without materializing another copy of the baseline. The converter receives this schema directly in a private registry; no external metadata lookup is required for the declared tables. Source primary-key changes are rejected, REPLICA IDENTITY FULL is required, and the pre-created pgoutput slot starts after baseline loading with no initial CDC snapshot.

Setup connection and native client execution have separate bounds. Each phase ends with a committed marker transaction; the controller waits up to two minutes for that transaction to reach the file writer, proving the preceding source prefix has been captured. It then requests normal source/decoder/writer shutdown, bounded to 60 seconds. Capture failure, a missing marker, incomplete transaction, failed writer, unexpected source-engine completion or failed shutdown prevents publication and retains diagnostics. A failed publication retains the closed source database. After publication succeeds, its database data directory is discarded.

## Retained input

A completed local CDC preparation retains:

- `manifest.json`: the final input catalog, published after normal capture close and validation.
- `native/Data_<sf>/`: the original baseline CSVs and the native client's bound runtime inputs.
- `schema.sql` and `pixels-schema.tsv`: the source DDL and ordered Pixels type/primary-key specification.
- `cdc/00000000.proto`, subsequent numbered files and `cdc/files.json`: whole-transaction row changes and the native writer's closed-file receipt.
- Effective configuration, build identities, client summaries, phase boundaries and diagnostics, retained locally for reproducing the capture.

Reusable CSV, CDC and Pixels baseline manifests are limited to 16 MiB. Their file descriptors use canonical relative paths, sizes and SHA-256 digests; counts must be nonnegative JSON integers. Symbolic links, path aliases and traversal are rejected.

The final manifest owns the reusable input inventory. It binds baseline row counts and files, schema/DDL, the ordered CDC file list, physical sizes/SHA-256 hashes, transaction counts and source/client/build/configuration evidence. `cdc/files.json` is a writer receipt, not an independently published input. Input files are relative to their root, so the directory can be moved or copied without retaining the preparation database or original absolute paths. `check-cdc-input` verifies the published catalog, baseline content/counts, file framing, schema bindings, transaction/row completeness and hashes; symbolic links, path aliases/traversal, fractional counts, missing/unlisted/reordered files and unsupported operations fail.

The engine's sink-properties overrides may set `sink.proto.maxRecords` to a positive 32-bit integer. During preparation it limits complete transactions per file; it never splits a transaction. Its default belongs to the pinned sink configuration.

Each file contains complete transactions. Each frame is a big-endian signed 32-bit key, a big-endian signed 32-bit payload length, then a protobuf message from the pinned Pixels `sink.proto`. Key `-1` identifies TransactionMetadata; other keys identify RowRecord. Individual frames are limited to 16 MiB. Transaction frames appear as BEGIN, globally ordered rows, END. Global/table row positions and exact END counts must agree. UPDATE/DELETE carry complete before images; INSERT/UPDATE carry after images.

Row-frame table IDs are capture-local ordinals in `pixels-schema.tsv`, starting at 1. They are not physical table IDs of a later Pixels deployment. Source database/schema/table names remain in RowRecord; a target integration must bind those names and types to its fresh metadata. Source XIDs and source event timestamps are retained as source information, not as Index Trace versions or replay-wall-clock freshness measurements.

`counts.transactions` includes the marker transactions. `counts.changes` includes all row changes, `counts.businessChanges` excludes marker rows, and `counts.markers` counts the one-row marker transactions. They are distinct from distinct business rows, Index Trace operations and service RPCs. The marker report acknowledges file-writer acceptance; normal file close and final validation establish input publication. Neither signal measures target visibility or remote durability.

## COS publication and local materialization

`publish-input` and `fetch-input` use the S3-compatible profiles in `config/index/storage.yaml`. Configure the bucket/endpoint and supply the named credential environment variables. They accept CSV inputs, CDC inputs and Pixels baseline manifests. The latter bind columnar files already stored on COS; transfer does not relocate those files.

```bash
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos \
  sibench/inputs/hybench results/capture/hybench-csv
# Use manifest_sha256 from the publication result, not the inner CSV/CDC manifest digest.
java -jar build/sibench/sibench.jar fetch-input config/index/storage.yaml cos \
  sibench/inputs/hybench MANIFEST_SHA256 results/inputs/hybench-csv
```

The same commands publish and fetch a completed CDC input. Only manifest-bound payloads and the canonical `manifest.json` are transferred. Local diagnostics, service settings and other unlisted files remain local. Retain the original generation evidence separately when reproducing source execution is required.

Objects are addressed by SHA-256 below `OBJECT_PATH/objects/`; the final transfer inventory is published at `OBJECT_PATH/manifests/<sha256>.json`. CSV and CDC inputs using the same object path share identical content objects. Repeated publication checks existing object sizes, sends missing objects, and republishes the identical inventory. It does not depend on conditional PUT, directory rename or an exclusive writer. Keep these keys immutable and restrict writes to trusted producers. A successful PUT is an acknowledgment; it does not certify provider durability or cross-instance visibility. Download verifies sizes, digests and the input's semantic checks before succeeding.

Downloads require a fresh directory, write the canonical manifest last, and never overwrite an existing cache. On failure they retain partial files for diagnosis and remove the canonical manifest. To reuse an existing local input, run its `check-csv-input`, `check-cdc-input` or `check-pixels-baseline` check; do not fetch over it. Failed uploads may leave unreferenced content objects. These commands do not delete remote objects or garbage-collect shared inputs.

Files larger than 64 MiB use bounded streaming multipart upload; failed multipart transfers are aborted. Requests have a ten-minute total bound per API call and a three-minute attempt bound. Whole-artifact transfer time depends on input size and file count.

## Current boundary

The [Pixels experiment consumer](pixels-experiment.md) imports a separately prepared COS columnar baseline, rebinds source names to fresh metadata, and applies all files once or a complete transaction prefix. `prepare-pixels-inputs` runs PG capture and Pixels baseline preparation concurrently from the same sealed CSV. Trace export, business queries and sink freshness are separate experiment settings. `generate-pixels` is the live PostgreSQL → sink → Pixels Index Trace workflow; `generate` captures native engines directly. Ordinary isolated index replay consumes Index Trace alone. Input preparation establishes reusable inputs; target correctness and performance require separate evaluation.
