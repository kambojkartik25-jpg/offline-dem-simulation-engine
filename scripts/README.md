# Scripts Folder

Utility scripts for local execution, diagnostics, and standalone optimization workflows.

## Notable Script

- `optimize_standalone_db.py`
  - Runs optimization against DB-backed state outside the UI flow.
  - Useful for debugging and reproducible performance checks.
- `export_top_candidates_csv.py`
  - Runs `/api/optimize` repeatedly, defaults to `200` runs, and writes the top `4` optimized candidates per run to CSV.
  - Useful for Monte Carlo style comparison of optimization outputs across seeds.

## Usage Guidance

- Prefer API paths for product behavior validation.
- Use scripts for developer diagnostics and batch checks.

