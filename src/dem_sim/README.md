# dem_sim Module

Core backend module for DEM-style silo discharge simulation, blending, and optimization.

See also:
- API details: [API.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/src/dem_sim/API.md)
- Config reference: [CONFIG.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/src/dem_sim/CONFIG.md)
- Architecture: [ARCHITECTURE.md](/c:/Updated_Silo_discharge/offline-dem-simulation-engine/ARCHITECTURE.md)

## Responsibilities

- API routing and orchestration (`web.py`)
- Physics and blending computation (`model.py`)
- Service dispatch (`service.py`)
- State mutation and inventory lifecycle (`state.py`)
- DB access and schema (`db.py`, `schema.py`)
- Validation and reporting (`reporting.py`)
- Fill/charging logic (`charger.py`)

## Inputs

- Silos
- Layers
- Suppliers/COA
- Incoming queue
- Discharge plan
- Run configuration and target parameters

## Outputs

- Predicted discharge and blended quality parameters
- Ranked optimization candidates
- Updated inventory/state after apply
- Event and result persistence records

