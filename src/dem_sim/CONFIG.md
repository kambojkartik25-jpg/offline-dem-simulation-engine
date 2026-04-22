# Configuration Guide (dem_sim)

Last Updated: 2026-04-22


## Runtime Environment Variables

| Variable | Purpose | Example |
|---|---|---|
| `DEM_SIM_DATABASE_URL` | PostgreSQL connection string used by `db.py`/`schema.py` | `postgresql://user:pass@localhost:5432/dem_sim` |

If `DEM_SIM_DATABASE_URL` is absent, DB-backed features fail.

## API/Optimizer Tuning (Code-Level Defaults)

Configured mainly in `src/dem_sim/web.py` and `src/dem_sim/service.py`.

Important knobs:
- `iterations` (search breadth)
- `config.steps` (simulation timesteps, performance-sensitive)
- `DISCHARGE_FRACTION_MIN/MAX` (search bounds)
- `FIXED_DISCHARGE_TARGET_KG` (required discharge per brew)
- `PARAM_WEIGHTS` and `DEFAULT_PARAM_RANGES` (objective behavior)

## Physics Config (RunConfig)

Defined in `src/dem_sim/service.py` and used by `model.py`:
- `rho_bulk_kg_m3`
- `grain_diameter_m`
- `beverloo_c`
- `beverloo_k`
- `gravity_m_s2`
- `sigma_m`
- `steps`
- `auto_adjust`
- optional: `moisture_beta`, `sigma_alpha`, `skew_alpha`

## Recommendation

- Keep operational defaults in one config profile per environment (dev/test/prod).
- When changing target mass, lot size, or silo capacity, validate end-to-end with:
  1. `/api/data/generate-random`
  2. `/api/process/run_simulation`
  3. `/api/process/optimize`

## Related Docs

- Module overview: [README.md](README.md)
- API contracts: [API.md](API.md)
- Root runtime context: [../../README.md](../../README.md)

