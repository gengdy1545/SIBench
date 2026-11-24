# Working agreement

## Scope and reading

- Work within the user's requested scope. Discussion, research and review do not
  by themselves authorize implementation, deployment or an experiment. Local
  notes describe decisions and evidence; reading them does not activate old tasks.
- If `develop/README.md` exists, read its index and the current task first, then
  only relevant linked records. Local notes are optional and ignored; building,
  testing and understanding the delivered project must not depend on them.
- Run shell commands through `rtk`. Use CodeGraph for symbols, callers/callees and
  impact when its index is usable; use `rtk rg` for exact text, configuration or a
  missing/stale index. Index only when needed and source reads are authorized.
- Verify behavior in source and actual consumers. Existing code, design proposals,
  passing tests and historical results are different kinds of evidence.
- Proceed autonomously within the authorized task, without another model's
  sign-off. Before implementation, state the evidence, affected behavior and
  smallest meaningful validation. Raise unresolved research-semantic choices or
  costs beyond scope; do not create a plan document for every edit.
- Preserve unrelated changes, staging boundaries, active jobs and fixed comparison
  builds. Notes do not grant permission to stage, commit, deploy, update submodules
  or take over processes.
- Report actual coverage and unexamined areas. Repository-wide cleanup or merge
  readiness requires the intended diff, relevant current code and consumers,
  build/test entry points, configuration and documentation; a search or unit suite
  alone is insufficient.

## Repository and configuration

- Organize production code, tests, configuration and documentation by responsibility,
  using stable names. Avoid `phase1`, `step2`, `misc`, `temp`, `new` and `final`
  directories. Research iterations belong in ignored notes, not product structure.
- Keep benchmark sources in `benchmark/`, index engines in `index/`, and general
  source dependencies in `third_party/`, all as pinned submodules. New submodules
  record the verified default branch in `.gitmodules`; gitlinks own pins, and
  builds/tests never upgrade them automatically. Preserve upstream licenses.
- Use JDK 17 and root Maven. Java core code belongs in
  `src/main/java/org/sibench/`, Java tests in `src/test/java/org/sibench/`.
  Build core, benchmarks and index engines into `build/sibench/`,
  `build/benchmarks/` and `build/index/`; local service state belongs in
  `build/services/`. Historical build locations belong in local records.
- Java owns configuration, generation, Trace interpretation, scheduling, replay
  adaptation and result validation. Native instrumentation and thin bindings belong
  in `src/main/native/`; backend repositories own engine mechanisms. Integration
  does not authorize redesigning an engine. Python/Shell orchestrate the Java CLI
  and perform offline analysis/reporting without duplicating core semantics.
- Keep build/capture/report entry points in `scripts/`; plotting scripts, styles
  and dependencies/usage in `graph/scripts/`, `graph/config/` and `graph/`.
  Manual probes may use `develop/scripts/`; required checks cannot depend on it.
- Keep one public configuration per benchmark in `config/benchmark/`, named for
  its source without a scale suffix. Defaults use 87,060,000 baseline rows
  (HyBench SF10); exact scale belongs in configuration and effective snapshots.
  Put backend settings in `config/index/`, one engine file/folder in `config/engine/`
  and runtime selection/settings in `config/sibench.yaml`. Use explicit settings,
  not inheritance or public scale/client/mode variants; experiments use
  `develop/configs/`, tests use resources or temporary files.
- Test scale names use actual row counts (`1k`, `2k`, `1b`) or upstream scales
  (`sf1`), not `small`/`large`. Retain required inputs, use placeholders for runtime
  locations, and construct case-specific variants in memory or temporary files.
- Give each setting and dependency pin one owner. Use the consuming language's
  build system for published dependencies and gitlinks for source dependencies;
  runtime configuration is not a dependency inventory. Derive copies and preserve
  effective run snapshots.
- Build PostgreSQL, DuckDB, RocksDB and Pixels/pixels-sink generation engines from
  pinned `third_party/` sources. Apply `src/main/native/` patches to build copies,
  leaving upstream clean. Bind matching headers, libraries and JNI/Java artifacts
  to build identities; Maven consumes/packages them without a separate upstream
  runtime pin or prebuilt fallback. Choose other dependency forms by integration need.
- Ship necessary Java, native and Python tests, fixtures, fault injection and
  synthetic responses under `src/test/`, outside production artifacts and paths.
  Native-language test modules may use their owning build layout if excluded from
  production compilation. Do not add runtime flags, mutable hooks or validation
  bypasses solely for tests; injection abstractions must serve production
  responsibilities. Preserve independent oracles and documented test entry points.
- Java classes/files use UpperCamelCase and packages lowercase; proto, C/C++,
  Python and configuration names use lower_snake_case; Shell/Markdown names use
  lower-kebab-case. Preserve conventional, upstream and generated names, and
  release-named patches such as `postgresql-14.24.patch`. Do not mass-rename for
  appearance; necessary renames update all consumers together. Keep domain terms
  consistent across languages.

## Research ideation and assessment

- Apply this workflow to idea generation, research positioning, novelty review
  and requested architecture exploration, not automatically to routine fixes.
  Keep valuable problems open while questioning mechanisms and claims. Similar
  work is neither an automatic rejection nor evidence of novelty.
- Develop the strongest plausible formulation, then actively seek counterevidence.
  Separate external requirements from changeable design choices. Hypotheses may
  start from mechanism analysis or cost models; label them unverified rather than
  requiring an implementation or measured benefit before discussion.
- Investigate the closest relevant primary sources within a stated search scope.
  Check problem settings, assumptions, guarantees, mechanisms and contribution
  boundaries in the paper's substance, not only titles/abstracts. Cite precise
  evidence for decisive comparisons; disclose inaccessible sources, uncertain
  interpretations and missing coverage. No search proves absolute novelty.
- Compare strong relevant baselines and straightforward adaptations/combinations
  of existing methods. Identify inherited ideas and the actual added contribution.
  A different name, deployment or combination needs an explanation of the meaningful
  difficulty it resolves. Do not manufacture novelty through weaker semantics,
  extra resources or selective comparisons.
- Assess these dimensions separately, without hiding decisive gaps in a total score:

  | Dimension | Required question |
  | --- | --- |
  | Novelty | What contribution is not already covered by the closest work? |
  | Significance | Which meaningful need or limitation does it address, and under what conditions? |
  | Feasibility | What mechanism could deliver it, with which correctness, cost and capacity risks? |
  | Evaluability | Which baseline, ablation or counterexample distinguishes the claim, and what would refute it? |

- Write claims only as strongly as evidence permits. Outcomes may be continue,
  revise the mechanism/claim, reject the current claim as covered, or insufficient
  evidence. Name concrete overlaps when rejecting and substantive new distinctions
  when revising; do not endlessly repackage a covered idea.
- Choose the next investigation by the uncertainty it resolves. Distinguish sources,
  facts, inferences, hypotheses and experimental results. Negative results apply to
  tested mechanisms and conditions, not automatically to the whole problem.
  Research proposals do not authorize code changes or costly runs.

## Implementation and compatibility

- Maintain the confirmed design during implementation unless changing it is in
  scope. Research may challenge it; an untested proposal is not implemented behavior.
  Public contracts live in `docs/`.
- Preserve native generation's lifecycle: one fresh database of the explicitly
  selected engine for loading, RowIDs and capture; reject existing work/output
  directories; preserve native transactions/snapshots. Publish Trace after normal
  host shutdown, then delete the working database; retain only working data needed
  for export. Keep failure diagnostics; do not add recovery or reusable initial-state
  caches without need. Runtime benchmark
  entries own selected Trace paths, generation switches and upload URIs; analysis
  and replay share them without another cache. Shared AWS/COS settings and
  environment-variable credential references belong in runtime configuration;
  URI schemes select stores. Keep one
  current selection per benchmark/workload.
- When replacing a design, update implementation, actual consumers, configuration,
  builds, scripts, tests, fixtures and documentation together. Remove superseded
  paths without unrelated cleanup or speculative migration frameworks. Preserve
  current invariant/rejection tests, independent oracles and useful diagnostics.
- Do not retain superseded APIs, readers, fallbacks or transition flags for
  hypothetical compatibility. Before removal, identify real consumers and whether
  necessary artifacts can be regenerated/migrated without invented facts. Obtain
  a user decision if necessary data or reproducibility would be lost. Retained
  compatibility names its consumer, reason and removal condition; prefer bounded
  migration over permanent dual paths.
- Trace and Index Service are one coordinated implementation with one supported
  format/protocol. Add version negotiation only for an established independent
  consumer or compatibility requirement, not to label development iterations.
  Preserve type/layout/integrity checks, commit/snapshot versions, dependency
  versions and build/input identities; judge version-like fields by their role.

## Validation

- For documentation-only work, check links, references and consistency. For code,
  run targeted tests first and root `rtk mvn test` before delivering a coherent
  task, plus affected native/Python/build checks. Broaden or repeat only for relevant
  changes, failures or unresolved risks. Unit tests do not establish real-service
  correctness, a particular run's inputs or performance.
- Semantic changes need a counterexample or independent validation distinguishing
  intended from old behavior. Derive critical expectations from hand-checkable
  cases, original database observations or independent oracles. Bug regressions
  must detect the old defect; validators must reject deliberately wrong results.
  Do not weaken assertions, skip failures or regenerate expectations just to pass.
  Byte-identical output establishes regression equivalence only.
- Tests protect current invariants, not historical implementation details. Remove
  abandoned semantics and consolidate duplicates only while preserving distinct
  failure modes. Fixed bugs do not make counterexamples obsolete; test counts or
  checks mirroring implementation are not correctness evidence.
- Cover generation, capture, extraction, Trace compilation and replay. Each
  production check needs a failure, responsible boundary, trigger and cost.
  Preserve trust-boundary validation, input integrity, source completeness, service
  capabilities, result correctness and data-loss prevention. Remove redundancy
  only after verifying enforced invariants; fuse checks with necessary reads and
  reuse evidence only when input/configuration/semantic identities still match.
- Keep production generation, backend evaluation and independent acceptance as
  explicit workflows with distinct triggers and costs. Normal generation fails
  with diagnostics; do not attach development oracles or cross-host recovery to
  every run. Independent audits may be explicit acceptance steps only while runtime
  checks enforce the contract; required validation must not silently become optional.
- Reuse inputs at the changed boundary: backend work normally reuses Trace,
  compiler work reuses available raw capture, capture work needs affected source
  runs. Small cases cover combinations/interleavings; representative full inputs
  cover complete results and cumulative resources. Add large variants for distinct
  risks, not every axis. Short tests cannot replace required scale, recovery or
  end-to-end acceptance; state which existing evidence remains valid.
- Separate source registration, build/deployment, semantic integration and full
  acceptance. Registration does not enroll/certify a backend. Before comparison,
  verify read timestamps, retained versions, deletion, Abort, transaction-local
  reads and complete-commit publication. Native snapshots/batches alone are
  insufficient. Resolve unsupported scope without dropping operations or weakening
  semantics; keep the selected roster in the current work record.
- Verify publication primitives on the actual deployment; S3-compatible access
  does not establish atomic publication or fencing. Measure ACK, remote durability
  and cross-instance visibility separately. Align resource/cache budgets and include
  adapters, coordination, storage and maintenance; separate storage environments
  and identify ports that change an upstream data path.

## Experiments and performance

- Start lightweight. Large captures, long benchmarks and paid runs need authorized
  inputs, resource budget, duration and stop conditions; a local note is not run
  authorization. Bound setup, execution and teardown separately.
- Before optimizing an implementation, identify a measured bottleneck, expected
  end-to-end benefit and bounded comparison. Fix a correct comparison build, inputs,
  effective configs and results before a batch of changes; label missing metrics
  and fill essential gaps with bounded runs. Compare the same input, environment,
  cache conditions and correctness checks; record variation and resource costs.
- Keep changes attributable. Use short comparisons during iteration, investigate
  unexpected growth before stacking changes, then combine required full correctness
  acceptance with final time/space measurement. Do not run full capture after every
  micro-optimization. Noise and justified correctness costs must remain visible.
- Parallelize independent checks on separate resources within budget; isolate
  performance comparisons. Reuse initial backend state only where supported, binding
  identity, restore cost and cache conditions; measure fresh loading when under
  test. Do not add checkpoints merely to save runs.
- On failure, state a hypothesis, expected observation and stop condition for the
  smallest diagnostic retaining the failure conditions. Preserve the first error
  and collect diagnostics before stopping; distinguish it from drain/close waits.
  Repeat expensive acceptance only after a fix or new evidence, not to obtain an
  occasional pass for an unresolved intermittent failure.
- Stop or revert isolated optimization when benefit is noise or complexity outweighs
  it, preserving unrelated work and necessary correctness/capacity fixes. Limit
  no-gain conclusions to tested conditions. Report before/after results, resource
  tradeoffs and the retain/revert decision.
- Every run has a unique ID/output and matching source/build, input, configuration,
  correctness and same-run resource evidence. Never overwrite or combine different
  runs' results/timing. Mark uncertain provenance unusable for combined conclusions.
  Report commands, identities, evidence paths and untested boundaries; distinguish
  implementation, partial validation and completed acceptance. No external sign-off
  is required beyond the user's authorization for the work.

## Documentation and development records

- Use English for delivered documentation, comments, docstrings, configuration
  explanations and CLI help. Preserve identifiers, required Unicode test data,
  upstream content and raw evidence. Chinese local notes may live only in ignored
  `develop/`; never stage/force-add them or link delivered docs to ignored records.
- Keep public README concise: prerequisites, usage, supported inputs and tests.
  Put external formats, service semantics, failure behavior and limits in `docs/`,
  organized by complete user workflows. Pixels input preparation/validation/transfer
  belong with Pixels experiments. Preserve public prerequisites when moving internal
  investigations, tuning, build details or detailed procedures into local notes.
- Give each maintained fact one home; summarize/link elsewhere. Update affected
  docs with behavior. AGENTS owns durable rules, not current tasks, machine settings,
  dependency inventories or experiment parameters. Publish claims only with matching
  evidence available to readers.
- Local records have explicit roles: README indexes work and procedures;
  `design.md` or `architecture.md` owns current design/rationale; the work list owns
  remaining actions; the results/acceptance index owns selected evidence and limits.
  Separate work/environment/storage files only for distinct responsibilities; do not
  rename established files merely to fit a template.
- Label proposed, implemented and validated states. Record meaningful research or
  design decisions under `iterations/` by topic when needed, not once per commit
  or minor edit. Include the question, alternatives/closest work where relevant,
  hypothesis, decision, evidence and unresolved uncertainty. Rejected approaches
  may remain when their reasoning supports a current decision; they are not a
  second active specification. Ordinary fixes need no separate iteration document.
- Maintain current sections in place, remove completed actions and superseded
  duplicate prose, and update consumers/links together. Backend notes own backend
  mechanisms; SIBench notes own shared inputs, contracts and integration. Reference
  shared facts instead of copying them; immutable experiment snapshots stay as run.

## Evidence retention

- Keep the latest complete acceptance for the current design until a replacement
  passes. Retain older evidence only for a named comparison, regression, active
  investigation or still-relevant design decision, including meaningful negative
  results. Do not accumulate every run/commit or discard useful reasoning merely
  because implementation changed. Remove claims whose evidence is lost.
- Keep required input/build identities, effective configurations, correctness and
  same-run resource measurements, plus inputs/certifications needed for regeneration.
  A comparison binary is evidence, not a reason to retain superseded source/readers.
  Preserve effective snapshots before removing one-run public configurations.
- Check consumers, canonical inputs and recoverable copies before pruning. Verify
  published counterparts before deleting the sole local copy; obtain decisions for
  required reproducibility losses under the compatibility rule and record accepted
  losses. Keep one necessary comparison binary/diagnostic state, not all candidate
  builds, repeated indexes, caches or superseded derived summaries.
- Preserve selected raw reports, logs and manifests as recorded. Correct derived
  analysis separately; document material pruning/relocation and preserve meaningful
  failures. Required tests and validation stay delivered, not in ignored notes.
  Evidence retention needs no additional archival framework.
