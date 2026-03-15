# Offline DEM Simulation Engine

Production-style application for estimating discharged blend composition from layered malt lots in silos.

## Features
- Beverloo mass-flow model for each silo.
- Layer-to-height conversion for lot segments.
- Gaussian mixing sampler over moving discharge front.
- Multi-silo discharge simulation and total blend parameter calculation.
- CLI workflow for generating sample data, validating inputs, running simulation, and exporting artifacts.

## Project structure
- `src/dem_sim/model.py`: physics + blending engine.
- `src/dem_sim/service.py`: run orchestration and config object.
- `src/dem_sim/io.py`: CSV input loading and output directory helpers.
- `src/dem_sim/reporting.py`: summaries and output artifact writers.
- `src/dem_sim/sample_data.py`: built-in sample dataset generator.
- `src/dem_sim/synthetic.py`: synthetic dataset generation within malt COA ranges.
- `src/dem_sim/cli.py`: command-line interface.
- `src/dem_sim/web.py`: FastAPI app and API endpoints.
- `src/dem_sim/ui/index.html`: web app shell.
- `src/dem_sim/ui/styles.css`: responsive UI styling.
- `src/dem_sim/ui/app.js`: frontend behavior and API integration.
- `scripts/run_example.py`: script entrypoint using in-code example dataframes.
- `tests/test_smoke.py`: smoke test over sample-data flow.
- `tests/test_web_api.py`: web/API smoke coverage.
- `Silo_discharge.ipynb`: original R&D notebook.

## Install
```bash
python -m pip install -e .
```

Or install dependencies only:
```bash
python -m pip install -r requirements.txt
```

## CLI usage
Create sample input files:
```bash
dem-sim init-sample --out data/sample
```

Create synthetic input files:
```bash
dem-sim init-synthetic --out data/synthetic --seed 42 --silos 3 --suppliers 3 --lots 8
```

Validate input files:
```bash
dem-sim validate --in data/sample
```

Run simulation:
```bash
dem-sim run --in data/sample --out outputs/latest --auto-adjust
```

`--steps` guidance (trade-off between speed and smoothness):
- `400-800`: quick exploratory runs
- `1200-2000`: default production-style planning (good balance)
- `3000+`: high-fidelity sensitivity checks (slower)

Run without installing as a script:
```bash
PYTHONPATH=src python -m dem_sim run --in data/sample --out outputs/latest --auto-adjust
```

## Web UI / API
Start the server:
```bash
dem-sim-web --host 127.0.0.1 --port 8000
```

Or without script install:
```bash
PYTHONPATH=src python -m dem_sim.web --host 127.0.0.1 --port 8000
```

Open:
- `http://127.0.0.1:8000/` (full web UI)
- `http://127.0.0.1:8000/docs` (Swagger UI)

### UI quick start (operator flow)
1. `Load Sample`
2. `Validate Inputs`
3. `Run Simulation`
4. Configure optimization target + preset/iterations/seed
5. `Optimize Blend`
6. Review:
- KPI strip
- Top candidates
- What changed
- Scenario compare
- Explainability and convergence snapshot

Keyboard shortcuts:
- `Ctrl/Cmd + Enter`: run simulation
- `Ctrl/Cmd + Shift + O`: optimize blend

Main endpoints:
- `GET /health`
- `GET /api/sample`
- `POST /api/validate`
- `POST /api/run`
- `POST /api/optimize` (searches discharge fractions toward target COA)
  - Uses normalized weighted L2 objective (error scaled by COA parameter ranges)
  - Search strategy: stratified exploration + local refinement around current best
  - Returns best plan plus Top-5 candidate plans

## Input files
Place these CSV files in the input directory:
- `silos.csv`
- `layers.csv`
- `suppliers.csv`
- `discharge.csv`

## Output artifacts
Generated in output directory:
- `segment_contributions.csv`
- `lot_contributions.csv`
- `segment_state_ledger.csv` (initial/discharged/remaining per segment)
- `lot_state_ledger.csv` (initial/discharged/remaining per lot per silo)
- `silo_state_ledger.csv` (initial/discharged/remaining per silo)
- `summary.json`

## UX metric targets
- Time to first valid run: `<= 180s`
- Validation issue identification: `<= 10s` median
- Optimize decision time: `<= 20s` median
- Run failure rate: `< 2%`
- Optimize failure rate: `< 3%`

See `docs/ui-ux-m8-handoff.md` for rollout checklist and metric definitions.
## PostgreSQL Persistence (Optional)
The API can persist state snapshots, stages, history, and run/optimization results to PostgreSQL.

1. Set database URL before starting server:
```bash
set DEM_SIM_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dem_sim
```

2. Start server normally. On startup, schema is auto-created if missing.

Tables created automatically:
- `sim_snapshots` (full state + summary per key event)
- `sim_stages` (deduped stage timeline)
- `sim_history` (deduped state history)
- `sim_results` (`/api/run`, `/api/optimize`, discharge prediction payloads)

If `DEM_SIM_DATABASE_URL` is not set, app behavior remains unchanged (in-memory only).
