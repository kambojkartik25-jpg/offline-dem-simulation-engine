# API Reference (dem_sim)

Backend is served by FastAPI in `src/dem_sim/web.py`.

## Authentication

- Current implementation is local/offline oriented and does not enforce auth at API layer.
- If deployed externally, add auth middleware/gateway before production usage.

## Key Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Service health check |
| GET | `/api/sample` | Load bootstrap sample payload |
| POST | `/api/data/generate-random` | Generate and seed random data |
| GET | `/api/state` | Current state and summary |
| POST | `/api/state/reset` | Reset state to sample/bootstrap |
| POST | `/api/process/run_simulation` | Fill-only simulation |
| POST | `/api/process/optimize` | Optimize using latest state |
| POST | `/api/process/apply_discharge` | Apply selected discharge plan |
| POST | `/api/validate` | Validate request shape/COA |
| POST | `/api/run` | Single prediction run for given discharge |
| POST | `/api/optimize` | Direct optimize endpoint |
| POST | `/api/schedules/generate` | Create/recreate brew schedule |
| GET | `/api/schedules/{schedule_id}` | Fetch schedule and items |
| POST | `/api/schedules/{schedule_id}/items/{brew_id}/optimize` | Optimize schedule item |
| POST | `/api/schedules/{schedule_id}/items/{brew_id}/apply` | Apply selected scenario |

## Primary Request Models

- `RunRequest`
- `OptimizeRequest`
- `ProcessRunSimulationRequest`
- `ProcessOptimizeRequest`
- `ProcessApplyDischargeRequest`
- `GenerateRandomDataRequest`
- `GenerateScheduleRequest`
- `ScheduleOptimizeRequest`
- `ScheduleApplyRequest`

Defined in `src/dem_sim/web.py`.

## Error Patterns

- `422` for invalid payload/constraints/COA violations
- `404` for missing schedule/schedule item
- `500` for unhandled backend exceptions

