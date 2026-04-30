# Tests Folder

Last Updated: 2026-04-22


Contains test coverage for simulation, optimization behavior, API contracts, and state transitions.

## Scope

- physics/model behavior checks
- API endpoint behavior and validation responses
- schedule and discharge workflow tests
- state mutation and persistence-related assertions

## Run

Use project test command (for this repo typically `pytest`).

### Quick Verification Commands

Run these checks before sharing changes:

```powershell
$env:PYTHONPATH="src"
.\.venv\Scripts\python.exe -m pytest -q tests/test_config_runtime.py
```

Config + API smoke checks:

```powershell
$env:PYTHONPATH="src"
.\.venv\Scripts\python.exe -m pytest -q tests/test_smoke.py tests/test_process_run_simulation.py
```

Production-plan apply behavior (requires DB-backed runtime):

```powershell
$env:DEM_SIM_DATABASE_URL="postgresql://postgres:<password>@localhost:5432/dem_sim"
$env:PYTHONPATH="src"
.\.venv\Scripts\python.exe -m pytest -q tests/test_web_api.py::test_production_plan_apply_tracks_reason_and_scenarios
```

## Related Docs

- Root overview: [../README.md](../README.md)
- Module API references: [../src/dem_sim/API.md](../src/dem_sim/API.md)

