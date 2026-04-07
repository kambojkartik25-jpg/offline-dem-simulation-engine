# UI Module

Frontend static assets for the simulation and optimization workflow.

## Files

- `index.html`: base page layout and script/style includes.
- `app.js`: API calls, UI state management, rendering of candidates/schedule/state.
- `styles.css`: visual styling.

## Backend Dependencies

UI calls endpoints in `src/dem_sim/web.py`:
- `/api/state`
- `/api/process/run_simulation`
- `/api/process/optimize`
- `/api/process/apply_discharge`
- `/api/schedules/*`

## Contract Sensitivity

- UI rendering expects specific response structures for candidates and blended params.
- Keep backend response shape changes documented in:
  - [src/dem_sim/API.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/src/dem_sim/API.md)
  - [CHANGELOG.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/CHANGELOG.md)

