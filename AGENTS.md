# Repository organization

- Organize code, tests, configuration, benchmarks, and documentation by domain,
  responsibility, or purpose. Do not introduce permanent `phase1`, `step2`, or
  other development-stage directories.
- Use stable names that describe the contents or behavior. Avoid `misc`, `temp`,
  `new`, and `final` as organizational names.
- Keep upstream benchmark sources in `benchmark/` submodules. Store generated
  builds under `build/benchmarks/` and local service state under `build/services/`.
- Keep index-backend upstream sources in `index/` submodules, pinned by gitlinks,
  and general third-party source dependencies in `third_party/` submodules.
  Document build prerequisites and entry points in README; preserve upstream
  licenses in the submodules. Keep historical toolchains and test-service binary
  identities in local development notes. Build index backends and their
  dependencies into `build/index/`.
- Keep one public configuration per benchmark under `config/benchmark/`, named
  for the source without a scale suffix. Defaults use 87,060,000 baseline rows
  (HyBench SF10); exact scale stays in the file and each run's effective snapshot.
  Keep index/backend configuration under `config/index/`, and tool runtime
  settings, including PostgreSQL service settings, in `config/sibench.yaml`.
  Do not add scale, client-count or mode variants to the public directory.
  Keep experiment variants in `develop/configs/`
  and required test inputs in test resources or temporary directories.
  Test profiles distinguished by scale use actual total-row suffixes such as
  `1k`, `2k` or `1b`, or an explicit upstream scale such as `sf1`;
  avoid `small` and `large`. Keep only inputs needed by the tests and use test
  placeholders for required runtime locations; construct case-specific variants
  in memory or temporary files.
  Use explicit settings and simple orchestration, not an inheritance framework.
- Give each setting and dependency pin one authoritative owner; derive required
  copies rather than maintaining parallel values. Use the owning language's build
  system for managed dependencies and gitlinks for upstream source dependencies.
  Keep build dependencies with the component that consumes them; runtime
  configuration is not a dependency inventory. Preserve effective run snapshots.
- Implement the core in Java under `src/main/java/org/sibench/`, organized by
  responsibility, including configuration, generation, trace, replay and adapters.
  Keep Java tests under `src/test/java/org/sibench/` and core builds under
  `build/sibench/`. Use JDK 17 and the root Maven project.
- Keep necessary native instrumentation and thin backend adapters under
  `src/main/native/`. They may use C/C++, but must not duplicate the core Trace
  interpretation or workload semantics implemented in Java.
- Use Python/Shell for orchestration, offline analysis and reporting; call the
  Java CLI instead of duplicating core configuration or workload semantics.
- Keep build, capture, plotting and report entry points in `scripts/`. Put manual
  development probes in ignored `develop/scripts/`; required runtime validation
  and default tests must remain independent of that directory.
- Ship the tests, fixtures and checks needed to maintain delivered behavior,
  including native and Python tests under `src/test/`. Development-only execution
  or implementation language is not a reason to move them into `develop/`.
  Document their entry points and prerequisites; do not duplicate core semantics
  in orchestration tests, but preserve independent correctness oracles.
- Keep delivered Trace and Index Service contracts under `docs/`.
  Keep development guides, experiment notes and diagnostic configurations in the
  ignored `develop/` directory. Required test fixtures belong under test resources
  or temporary directories, never in `develop/`.
- Keep development milestones in local development notes, not directory names.
  Local plans, machine settings and experiment history are excluded from source
  delivery. Keep retained logs and manifests as recorded; document relocations
  separately.

## Naming

- Follow language conventions consistently: Java class files use UpperCamelCase
  matching the declared class, and Java packages use lowercase names. Proto,
  C/C++ source and header files, Python scripts and configuration files use
  lower_snake_case. Shell scripts and Markdown documents use lower-kebab-case.
- Preserve conventional names such as README.md, AGENTS.md and Makefile, upstream
  naming and generated filenames. Versioned patches retain their upstream release
  in the name, such as postgresql-14.24.patch. Apply these rules to new
  project-owned files; do not mass-rename existing files for appearance. Necessary
  renames must update callers, tests, fixtures, build files, scripts and
  documentation references together.
- Use the same domain terms across languages; casing and separators may differ.
  Do not introduce synonyms for the same concept without a semantic distinction.

## Development records

- If `develop/README.md` exists locally, read that index and the current task
  first, then load only the relevant linked notes. The directory is optional,
  ignored, and must not become a build or test dependency.
- Local development notes, when present, hold current goals, design choices,
  dependencies, remaining tasks, machine settings and experiment history. They
  are not a prerequisite for building, testing or understanding the repository.
- Integrate updates into the relevant current sections; remove completed tasks
  from remaining work. Separate implemented behavior, unverified choices and
  historical evidence. Keep the input/build identities needed by retained facts.
- README and contracts describe current behavior; local development guides
  describe detailed checks and measurement procedures. Do not link delivered
  documentation to ignored local notes or results. Publish experiment claims only
  with the matching reproducible evidence available to readers.

## Current design and compatibility

- Maintain the current design by default. Do not add or retain superseded APIs,
  format readers, compatibility flags, fallback implementations or transition
  branches for backward compatibility that has not been explicitly required.
- When replacing a design, update implementation, callers, configuration, build
  files, scripts, tests, fixtures and documentation together. Trace actual consumers
  beyond the initially reported file; remove the replaced path and its obsolete
  tests. Preserve tests of current invariants and rejection of unsupported inputs;
  implementation age alone does not make a diagnostic reference or independent
  oracle obsolete.
- Trace and Index Service currently ship as one coordinated implementation with
  one supported format and protocol. Do not add format revision fields, versioned
  protocol namespaces or legacy readers merely to record development iterations.
  Introduce explicit version negotiation only for an established independent
  consumer or compatibility requirement. This does not remove type/layout checks,
  integrity checks, data commit/snapshot versions, upstream dependency versions,
  or build/input identities. Judge other version-like fields by their actual role;
  do not delete them by name alone.
- Before removing an existing compatibility path, identify actual consumers and
  whether required artifacts can be regenerated or migrated without inventing
  missing facts. If removal would make necessary data unusable or destroy required
  reproducibility, explain the concrete impact and obtain a user decision on that
  tradeoff. Do not silently keep compatibility or discard required evidence.
- Any explicitly retained compatibility must name its consumer, reason and removal
  condition. Prefer a bounded migration when feasible over permanent dual paths.
  Keep selected reports, logs and manifests as recorded; retaining evidence does
  not require the current executable to support every historical format.
- A fixed comparison binary and its build/input identities are experiment evidence,
  not a reason to keep old source implementations or compatibility branches.
  Before removing one-run configurations from `config/`, preserve the effective
  snapshots needed to reproduce recorded runs and update maintained references;
  leave retained logs unchanged and document relocated evidence.
- Apply cleanup to the design being changed. Do not turn unrelated tasks into
  repository-wide legacy removal or introduce migration frameworks speculatively.

## Documentation responsibilities

- Use `README.md` for build, usage, supported inputs and test entry points.
  Keep only externally relevant format and service semantics in `docs/`.
  Put source investigations, detailed validation procedures, performance studies
  and environment notes in `develop/`, indexed for reading on demand.
- Update affected documentation with a behavior change. Keep current behavior and
  limits explicit; retain only the historical conclusions and evidence still needed.
  Keep machine-specific setup and changing experiment parameters out of AGENTS.md.

## Evidence retention

- Maintain the latest complete acceptance record for the current design. Replace
  superseded experiment prose and results after the new acceptance passes; do not
  accumulate a run history. Keep older evidence only for a retained comparison,
  current regression case or active investigation, and remove claims that lose
  their evidence. Preserve the inputs and certifications needed for regeneration.
- Retained conclusions need matching build/input identities,
  effective configuration, correctness results and same-run resource measurements.
- Keep one necessary copy of a comparison binary or diagnostic state. Do not retain
  every candidate build, repeated index, temporary cache, setup failure or superseded
  derived summary. Check consumers and required canonical inputs before pruning;
  do not discard unique facts needed by a retained conclusion or open investigation.
- Preserve the contents of selected raw reports, logs and manifests. Correct derived
  analysis separately, remove invalid/redundant derivatives, and describe material
  pruning or relocation in the relevant development record or commit description.
  Evidence retention does not require a new archival framework.

## Validation

- For documentation-only changes, check links, cross-references and consistency;
  do not run a Benchmark merely to validate prose.
- Review validation across benchmark data generation, capture, index-operation
  extraction, Trace compilation and Index Service replay. For each production
  check, identify the failure it detects, the boundary responsible for it, its
  trigger and its cost. Remove redundant checks or checks for failures prevented
  by enforced current invariants only after verifying that reasoning in the code.
  Fuse checks with necessary reads and reuse identity-bound evidence where valid;
  avoid repeated full scans or per-operation checks of already established facts.
- Preserve trust-boundary validation, input integrity, source completeness,
  service capability and result correctness checks, and errors that prevent data
  loss. Unit tests cannot certify a particular run's inputs or execution. Reuse
  requires matching inputs, configuration and relevant implementation semantics;
  do not assume mutable inputs still match prior evidence. Expensive independent audits
  may be explicit acceptance steps when the remaining runtime checks still enforce
  the contract; do not silently turn required validation into an optional probe.
- Tests protect current behavior and invariants, not historical implementation
  details. A fixed bug is not by itself a reason to delete its regression test.
  Remove tests for abandoned semantics and consolidate genuinely duplicate cases
  only when distinct failure modes remain covered. Keep counterexamples that can
  catch future regressions; do not count tests or mirror implementation logic as
  evidence of adequate coverage.
- During code iteration, run targeted tests first; run the root Maven `mvn test`
  check before delivering a coherent code task. Repeat broader checks only after
  relevant changes, failures or unresolved risks justify them. Record integration
  environments and results separately; unit tests do not establish real-service
  correctness or performance.
- For semantic changes, add a counterexample or independent validation that can
  distinguish the old behavior from the intended behavior. Matching old checksums
  or byte-identical recompilation establishes regression equivalence only.
- Derive critical expected results from hand-checkable cases, original database
  observations or an independent oracle, not solely from the implementation under
  test. For bug fixes, show that the regression detects the old defect; for result
  validators, demonstrate rejection of deliberately wrong results. Do not weaken
  assertions, skip failures or regenerate expected outputs just to make tests pass;
  any changed expectation needs a semantic justification and independent evidence.
- Reuse verified inputs at the affected boundary: backend changes normally reuse
  Trace, compiler changes reuse raw captures, and capture changes require affected
  source runs. Use short checks first and full validation when the affected boundary
  requires it. Small tests cannot replace required scale, recovery or end-to-end checks.
- Cover configuration combinations and controlled interleavings on small inputs;
  use representative full inputs to validate complete results and cumulative
  resource risks. Add large variants for distinct semantic or capacity risks,
  rather than multiplying every axis by default. Record what each run proves and
  which still-valid evidence it reuses; do not replace full-input correctness with
  a short test or preflight.
- Start with lightweight validation. Large captures, long benchmarks and paid
  experiments need an authorized scope with inputs, resource budget, duration and
  stop conditions; their presence in local notes alone does not authorize execution.
- When acceptance fails, switch to the smallest diagnostic experiment that retains
  the failing conditions. State the hypothesis, expected observation and stop
  condition before each run. Repeat expensive acceptance only when a fix or new
  evidence justifies it; an occasional pass does not resolve an intermittent failure.
- Distinguish the first failure from subsequent drain, close or process-cleanup
  waits. Bound setup, execution and teardown separately, preserve the original
  error and capture diagnostics before stopping the affected run. Do not treat a
  cleanup stack alone as proof of the original failure's cause.
- Give each experiment a unique run ID and output directory. Bind build, config
  and input identities; pair logs, results and timing only from that same run.
  Preserve meaningful failed evidence. Do not overwrite prior runs or combine a successful
  result with another run's timing; mark uncertain provenance unusable for combined
  conclusions until its identity is recovered. Use existing reports and simple
  orchestration rather than building a new experiment framework for this rule.

## Autonomous execution and optimization

- Proceed within the authorized task without requiring another model's review at
  each step. Before changing code, briefly identify the evidence, affected behavior
  and smallest meaningful validation; do not create a separate planning document
  for each edit. Raise unresolved research-semantic choices or costs beyond the
  authorized budget rather than silently changing scope.
- For performance work, identify a measured bottleneck, expected end-to-end benefit
  and a bounded comparison before optimizing. Compare the same input, environment,
  cache conditions and correctness checks; record variation and resource tradeoffs.
  Keep changes attributable. Do not run a full capture after every micro-optimization.
- Fix a correct comparison build, inputs, effective configs and available results
  before a batch of changes; label missing baseline metrics and fill essential gaps
  with bounded runs. Use short comparisons for hot-path or state changes during
  iteration, then combine full correctness acceptance with final time/space
  regression measurement. Investigate unexpected growth before stacking more
  changes; report noise and justified correctness costs, not only speedups.
- Parallelize independent checks on separate resources within their budgets.
  Isolate performance comparisons from competing experiments. Reuse a verified
  initial backend state only where the current backend supports it, recording
  identity, restore cost and cache conditions; measure fresh loading when loading
  itself is under test. Do not add checkpoint support merely to save a few runs.
- Stop or revert your isolated optimization when benefit is indistinguishable from
  noise or complexity outweighs the gain; do not revert unrelated work. Report a
  no-gain result instead of trying endless variants. Correctness fixes and required
  capacity improvements are not conditional on a throughput gain.
- At task delivery, report commands, input/build identities, results, untested
  boundaries and evidence paths concisely. For optimization, include before/after
  measurements and the retain/revert decision. Mark a stage complete only when its
  required acceptance checks pass; implementation, partial validation and blocked
  validation must remain distinguishable. No external supervisor sign-off is required.
- Claims of repository-wide cleanup or merge readiness require explicit coverage
  of the intended merge diff, relevant current code and consumers, build/test
  entry points, configuration and documentation. Report findings and unexamined
  areas against that scope; a local fix, text search or passing unit suite alone
  does not establish overall completion.
