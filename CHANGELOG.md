# Changelog

Last Updated: 2026-04-22


All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

Versioning and release style:
- Semantic versioning: `MAJOR.MINOR.PATCH`
- Keep entries scoped by Added/Changed/Fixed/Removed where applicable
- Record user-visible behavior changes and important operational changes

## [Unreleased]

### Added
- Repository documentation baseline:
  - `ARCHITECTURE.md`
  - `docs/system_technical_documentation.md`
  - module/service docs under `src/dem_sim/`
  - folder docs for `data/`, `scripts/`, `tests/`, and `outputs/`.
- README optimizer documentation updates:
  - fixed discharge target noted as 9,000 kg (not 12,000 kg)
  - explore range documented as [0.0, 1.0]
  - Brewmaster inference section added
  - fallback behavior for missing/failed inference endpoint documented
  - discharge fraction response semantics documented as mass-share of total discharge

### Changed
- Documentation only; no runtime logic changes in this changelog entry.

## Changelog Entry Template

```
## [x.y.z] - YYYY-MM-DD
### Added
- ...
### Changed
- ...
### Fixed
- ...
### Removed
- ...
```

