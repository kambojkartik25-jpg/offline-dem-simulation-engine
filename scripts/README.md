# Scripts Folder

Last Updated: 2026-04-22


Utility scripts for local execution, diagnostics, and standalone optimization workflows.

## Notable Scripts

- `export_top_candidates_csv.py`
  - Runs `/api/optimize` repeatedly, defaults to `200` runs, and writes the top `4` optimized candidates per run to CSV.
  - Useful for Monte Carlo style comparison of optimization outputs across seeds.
- `run_production_plan_batch.py`
  - Runs batched production-plan load/optimize/apply cycles against the API.
  - Useful for orchestration and throughput checks.
- `db_sanity_check.py`
  - Performs DB/API persistence sanity checks for optimize/apply flows.

## Usage Guidance

- Prefer API paths for product behavior validation.
- Use scripts for developer diagnostics and batch checks.

## Experiment Tracking

When running tuning or comparison experiments, log each run in:
- [EXPERIMENT_LOG.md](EXPERIMENT_LOG.md)

