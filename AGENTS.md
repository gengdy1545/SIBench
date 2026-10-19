# Repository organization

- Organize code, tests, configuration, benchmarks, and documentation by domain,
  responsibility, or purpose. Do not introduce permanent `phase1`, `step2`, or
  other development-stage directories.
- Use stable names that describe the contents or behavior. Avoid `misc`, `temp`,
  `new`, and `final` as organizational names.
- Keep upstream benchmark sources in `benchmark/` submodules. Store generated
  builds under `build/benchmarks/` and local service state under `build/services/`.
- Keep workload configurations in `config/workloads/<benchmark>/`, trace
  examples in `config/trace/`, and storage configuration in `config/storage/`.
- Implement the core in Java under `src/main/java/org/sibench/`, organized by
  responsibility, including configuration, generation, trace, replay and adapters.
  Keep Java tests under `src/test/java/org/sibench/` and core builds under
  `build/sibench/`. Use JDK 17 and the root Maven project.
- Use Python/Shell for orchestration, offline analysis and reporting; call the
  Java CLI instead of duplicating core configuration or workload semantics.
- Keep operational scripts (build, service management and experiment validation)
  under `scripts/benchmarks/`, `scripts/services/`, or
  `scripts/validation/`; documentation under `docs/benchmarks/`, `docs/contracts/`,
  or `docs/validation/` according to purpose.
- Development milestones belong in `plan.md`, not directory names. Dates and
  run identifiers may identify archived experiment evidence. Preserve historical
  logs and manifests as recorded; document relocations separately.
