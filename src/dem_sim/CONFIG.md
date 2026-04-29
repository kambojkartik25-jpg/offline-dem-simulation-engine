# Configuration Guide (dem_sim)

Last Updated: 2026-04-28

## Runtime Environment Variables

| Variable | Purpose | Example |
|---|---|---|
| `DEM_SIM_DATABASE_URL` | PostgreSQL connection string used by `db.py`/`schema.py` | `postgresql://user:pass@localhost:5432/dem_sim` |
| `DEM_SIM_PROFILE` | Runtime profile used to load `config/<profile>.json` | `local`, `dev`, `prod`, `training`, `inference` |
| `BREWMASTER_ENDPOINT_URL` | Brewmaster inference endpoint URL override | `https://.../score` |
| `BREWMASTER_API_KEY` | Brewmaster inference API key | `<secret>` |
| `BREWMASTER_VERIFY_TLS` | Brewmaster TLS verification override (`true/false`) | `true` |

Required vs optional:
- Required for DB-backed runtime: `DEM_SIM_DATABASE_URL`
- Optional integration: `BREWMASTER_API_KEY` (when absent, optimization still runs and endpoint scoring is skipped)
- Optional behavior override: `BREWMASTER_ENDPOINT_URL`, `BREWMASTER_VERIFY_TLS`, `DEM_SIM_PROFILE`

Failure behavior:
- Missing `DEM_SIM_DATABASE_URL` causes DB-backed operations to fail.
- Missing `BREWMASTER_API_KEY` does not fail the app; endpoint scoring is skipped.

## Local vs Cloud Secret Handling

- Local development:
  - Export variables from your shell or use compose environment substitution.
  - Do not commit real credentials.
- Cloud runtime:
  - Inject the same environment variables from the platform secret store.
  - No code changes are required for cloud secret injection.

## Profile-Based Config Files

Runtime profile configuration is loaded from repo-root `config/`:

- `config/base.json`
- `config/local.json`
- `config/dev.json`
- `config/prod.json`
- `config/training.json`
- `config/inference.json`

Loader: `src/dem_sim/config_runtime.py`.

## Config Precedence (Lowest to Highest)

1. Built-in defaults (`config_runtime.py`)
2. `config/base.json`
3. `config/<DEM_SIM_PROFILE>.json`
4. Environment variable overrides (where supported)
5. API request payload / CLI arguments

This preserves existing behavior: request-level runtime fields like `config`, `iterations`, and `seed` remain highest-priority overrides.

## API/Optimizer Tuning

Configured mainly in `src/dem_sim/web.py` and `src/dem_sim/service.py`.

Important knobs:
- `iterations` (search breadth)
- `config.steps` (simulation timesteps, performance-sensitive)
- `DISCHARGE_FRACTION_MIN/MAX` (search bounds)
- `FIXED_DISCHARGE_TARGET_KG` (required discharge per brew)
- `PARAM_WEIGHTS` and `DEFAULT_PARAM_RANGES` (objective behavior)

Profile-backed defaults now cover:
- `api.fixed_discharge_target_kg`
- `api.fixed_discharge_tol_kg`
- `api.default_steps`
- `brewmaster.endpoint_url`
- `brewmaster.timeout_s`
- `brewmaster.verify_tls`

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

- Keep operational defaults in per-environment profile files.
- When changing target mass, lot size, or silo capacity, validate end-to-end with:
  1. `/api/data/generate-random`
  2. `/api/process/run_simulation`
  3. `/api/process/optimize`

## Related Docs

- Module overview: [README.md](README.md)
- API contracts: [API.md](API.md)
- Root runtime context: [../../README.md](../../README.md)
