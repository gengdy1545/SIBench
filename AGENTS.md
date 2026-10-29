# Repository organization

- Organize code, tests, configuration, benchmarks, and documentation by domain,
  responsibility, or purpose. Do not introduce permanent `phase1`, `step2`, or
  other development-stage directories.
- Use stable names that describe the contents or behavior. Avoid `misc`, `temp`,
  `new`, and `final` as organizational names.
- Keep upstream benchmark sources in `benchmark/` submodules. Store generated
  builds under `build/benchmarks/` and local service state under `build/services/`.
- Pin index-backend upstream versions in `index/versions.yaml`; their sources are
  cloned under `index/` by the build script and built into `build/index/`.
- Keep maintained workload and service configurations directly in `config/`,
  without nested directories. Name workload files `<benchmark>_<scale>.yaml`;
  use descriptive filenames for trace examples and storage configuration.
  Store one-run effective configurations with that run's evidence, and test
  fixtures under test resources or temporary directories. Do not add permanent
  configurations distinguished only by date, port, output path or repeat number.
  Use existing templates and simple orchestration, not a new inheritance framework.
- Implement the core in Java under `src/main/java/org/sibench/`, organized by
  responsibility, including configuration, generation, trace, replay and adapters.
  Keep Java tests under `src/test/java/org/sibench/` and core builds under
  `build/sibench/`. Use JDK 17 and the root Maven project.
- Keep necessary native instrumentation and thin backend adapters under
  `src/main/native/`. They may use C/C++, but must not duplicate the core Trace
  interpretation or workload semantics implemented in Java.
- Use Python/Shell for orchestration, offline analysis and reporting; call the
  Java CLI instead of duplicating core configuration or workload semantics.
- Keep documentation under `docs/benchmarks/`, `docs/contracts/`, or
  `docs/validation/` according to purpose.
- Development milestones belong in `plan.md`, not directory names. Dates and
  run identifiers may identify selected experiment evidence. Keep retained logs
  and manifests as recorded; document relocations separately.

## Naming

- Follow language conventions consistently: Java class files use UpperCamelCase
  matching the declared class, and Java packages use lowercase names. Proto,
  C/C++ source and header files, Python scripts and configuration files use
  lower_snake_case. Shell scripts and Markdown documents use lower-kebab-case.
- Preserve conventional names such as README.md and AGENTS.md, upstream naming
  and generated filenames. Apply these rules to new project-owned files; do not
  mass-rename existing files for appearance. Necessary renames must update callers,
  build files, scripts and documentation references together.
- Use the same domain terms across languages; casing and separators may differ.
  Do not introduce synonyms for the same concept without a semantic distinction.

## Plan Maintenance

Plans should always reflect the current design and remaining work, rather than
accumulate historical implementation states. When updating a plan, integrate new
information into the appropriate existing context and remove obsolete or
redundant content.

- Keep current goals, design choices, capability limits, dependencies, remaining
  tasks and acceptance criteria in `plan.md`. Fold updates into their relevant
  sections rather than append review logs, superseded designs or deletion lists.
  Keep implemented behavior in README and contracts; link only the capability
  limits needed to understand the remaining work.
- The remaining-work section contains only unfinished tasks, dependencies and
  acceptance criteria. Remove completed tasks from it, including renamed sections
  describing their current implementation. Renumber remaining tasks from 1 and
  update references together; do not retain completed-task tables. Move standing
  requirements into constraints or acceptance criteria.
  After each update, check section placement, duplication and stale references
  throughout the plan, not just the edited paragraphs.
- Remove completed-work narratives from the plan; retain evidence in the relevant
  validation document only when it supports a current conclusion. Merging or
  deferring a task must preserve its valid requirements and acceptance criteria. Remove cancelled tasks and explain cancellations in
  the response or commit description; retain only rationale relevant to the
  current design in the plan.
- Distinguish research requirements, engineering choices and implemented behavior.
  A plan is not proof of correctness: identify conflicts with code, contracts or
  evidence and reconcile them as part of the relevant change. Do not describe
  planned or unverified behavior as implemented or validated.

## Current design and compatibility

- Maintain the current design by default. Do not add or retain superseded APIs,
  format readers, compatibility flags, fallback implementations or transition
  branches for backward compatibility that has not been explicitly required.
- When replacing a design, update callers, configuration, tests and documentation
  together and remove the replaced path and its obsolete tests. Preserve tests of
  current invariants and rejection of unsupported inputs; implementation age alone
  does not make a diagnostic reference or independent oracle obsolete.
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

- Use `README.md` for build and usage entry points, `docs/benchmarks/` for source
  workload guidance, `docs/contracts/` for detailed semantics and interfaces, and
  `docs/validation/` for experiment conditions, results and historical evidence.
  Link to these from the plan instead of duplicating their full contents.
- Update affected documentation with a behavior change. Keep current behavior and
  limits explicit; retain only the historical conclusions and evidence still needed.
  Keep machine-specific setup and changing experiment parameters out of AGENTS.md.

## Evidence retention

- Keep verified source inputs and certifications needed for regeneration, current
  acceptance results, meaningful comparisons, independent counterexamples and
  unresolved failures. Retained conclusions need matching build/input identities,
  effective configuration, correctness results and same-run resource measurements.
- Keep one necessary copy of a comparison binary or diagnostic state. Do not retain
  every candidate build, repeated index, temporary cache, setup failure or superseded
  derived summary. Check consumers and required canonical inputs before pruning;
  do not discard unique facts needed by a retained conclusion or open investigation.
- Preserve the contents of selected raw reports, logs and manifests. Correct derived
  analysis separately, remove invalid/redundant derivatives, and describe material
  pruning or relocation in the relevant validation document or commit description.
  Evidence retention does not require a new archival framework.

## Validation

- For documentation-only changes, check links, cross-references and consistency;
  do not run a Benchmark merely to validate prose.
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
  source runs. Follow the plan's short-flow and full-validation triggers. Small
  tests cannot replace required scale, recovery or end-to-end checks.
- Cover configuration combinations and controlled interleavings on small inputs;
  use representative full inputs to validate complete results and cumulative
  resource risks. Add large variants for distinct semantic or capacity risks,
  rather than multiplying every axis by default. Record what each run proves and
  which still-valid evidence it reuses; do not replace full-input correctness with
  a short test or preflight.
- Start with lightweight validation. Large captures, long benchmarks and paid
  experiments need an authorized scope with inputs, resource budget, duration and
  stop conditions; their presence in the plan alone does not authorize execution.
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
