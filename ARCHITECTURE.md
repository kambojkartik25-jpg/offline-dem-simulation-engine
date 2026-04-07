# Architecture

This service is an offline-first simulation and optimization engine for silo discharge and blend quality prediction.

See also:
- Root overview: [README.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/README.md)
- API contracts: [src/dem_sim/API.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/src/dem_sim/API.md)
- Full deep dive: [docs/system_technical_documentation.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/docs/system_technical_documentation.md)

## Architecture Diagram

![Silo Discharge Architecture Flow](docs/images/flow-diagram.jpg)

If the image does not render, place the diagram file at:
`docs/images/flow-diagram.jpg`

## High-Level Components

1. Frontend UI
- Files: `src/dem_sim/ui/*`
- Role: trigger simulation/optimization endpoints and display candidate scenarios.

2. Backend API (FastAPI)
- File: `src/dem_sim/web.py`
- Role: orchestration, validation, optimization loops, schedule endpoints, persistence calls.

3. Simulation/Optimization Core
- Files: `src/dem_sim/service.py`, `src/dem_sim/model.py`
- Role: execute discharge physics, layer contribution simulation, blended parameter estimation.

4. State and Inventory Engine
- File: `src/dem_sim/state.py`
- Role: in-memory state lifecycle, fill simulation, discharge application.

5. Persistence Layer
- Files: `src/dem_sim/db.py`, `src/dem_sim/schema.py`, `src/dem_sim/storage.py`
- Role: PostgreSQL access, schema management, event/result snapshots.

6. Data and Validation
- Files: `src/dem_sim/sample_data.py`, `src/dem_sim/synthetic.py`, `src/dem_sim/reporting.py`
- Role: seed/random data generation and input/COA validation.

## Request Flow (Text Diagram)

`UI -> FastAPI endpoint (web.py) -> validate inputs -> run_blend/model simulation -> score/rank (optimize path) -> persist events/results -> response`

## Core Runtime Paths

- Fill simulation: `POST /api/process/run_simulation`
- Single run: `POST /api/run`
- Optimization: `POST /api/optimize`
- Schedule optimize: `POST /api/schedules/{schedule_id}/items/{brew_id}/optimize`
- Apply discharge: `POST /api/process/apply_discharge`

## Data Persistence Summary

Main operational tables:
- `silos`
- `layers`
- `suppliers`
- `incoming_queue`
- `sim_events`
- `results_run`
- `results_optimize`
- `discharge_results`
- `brew_schedules`
- `brew_schedule_items`
