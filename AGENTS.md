# Repository organization

- Organize code, tests, configuration, benchmarks, and documentation by domain,
  responsibility, or purpose. Do not introduce permanent `phase1`, `step2`, or
  other development-stage directories.
- Use stable names that describe the contents or behavior. Avoid `misc`, `temp`,
  `new`, and `final` as organizational names.
- Keep upstream benchmark sources in `benchmark/` submodules. Store generated
  builds under `build/benchmarks/` and local service state under `build/services/`.
- Keep workload configurations in `configs/workloads/<benchmark>/`, trace
  examples in `configs/trace/`, and storage configuration in `configs/storage/`.
- Group scripts under `scripts/benchmarks/`, `scripts/services/`, or
  `scripts/validation/`; documentation under `docs/benchmarks/`, `docs/contracts/`,
  or `docs/validation/` according to purpose.
- Development milestones belong in `plan.md`, not directory names. Dates and
  run identifiers may identify archived experiment evidence. Preserve historical
  logs and manifests as recorded; document relocations separately.
