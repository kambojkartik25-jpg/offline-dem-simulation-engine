# Data Dictionary

Field-level guide for core business entities used by simulation and optimization.

## Silos

| Field | Type | Meaning |
|---|---|---|
| `silo_id` | string | Unique silo identifier |
| `capacity_kg` | float | Maximum silo capacity |
| `body_diameter_m` | float | Silo body diameter used in geometry mapping |
| `outlet_diameter_m` | float | Outlet diameter used in flow calculations |
| `initial_mass_kg` | float | Initial loaded mass at start state |

## Layers

| Field | Type | Meaning |
|---|---|---|
| `silo_id` | string | Parent silo |
| `layer_index` | int | Stack order in silo |
| `lot_id` | string | Source lot ID |
| `supplier` | string | Supplier/COA key |
| `segment_mass_kg` / `remaining_mass_kg` | float | Active mass for simulation |

## Incoming Queue

| Field | Type | Meaning |
|---|---|---|
| `lot_id` | string | Lot identifier |
| `supplier` | string | Supplier/COA key |
| `mass_kg` | float | Original lot mass |
| `remaining_mass_kg` | float | Residual not yet charged |
| `is_fully_consumed` | bool | Queue consumption status |

## Quality / COA Parameters

Common fields:
- `moisture_pct`
- `fine_extract_db_pct`
- `wort_pH`
- `diastatic_power_WK`
- `total_protein_pct`
- `wort_colour_EBC`

Additional optional fields may exist in DB/state payloads depending on configuration.

## Discharge

| Field | Type | Meaning |
|---|---|---|
| `silo_id` | string | Target silo |
| `discharge_mass_kg` | float/null | Absolute discharge mass |
| `discharge_fraction` | float/null | Fraction of available silo mass |

