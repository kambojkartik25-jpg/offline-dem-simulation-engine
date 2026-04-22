# Experiment Log

Last Updated: 2026-04-22


Track optimization experiments and tuning runs.

## Template

| Date | Scenario | Params Changed | Metrics | Outcome | Notes |
|---|---|---|---|---|---|
| YYYY-MM-DD | Example: 6 silos, 9t discharge | iterations=80, steps=800 | elapsed_ms, search_ms, best_score | Improved/Regressed | Key observation |

## Suggested Metrics

- `elapsed_ms`
- `timing_breakdown_ms.search_ms`
- feasible candidate count
- best objective score
- top candidate spread

## Related Docs

- Optimization behavior: [../README.md](../README.md)
- API contract for optimize: [../src/dem_sim/API.md](../src/dem_sim/API.md)

