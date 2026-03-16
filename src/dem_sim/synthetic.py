from __future__ import annotations

import csv
import random
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Physical constants
# ---------------------------------------------------------------------------
SILO_COUNT = 3
SILO_SLOT_COUNT = 4
SILO_CAPACITY_KG = 8000.0

# COA parameter order used throughout this module.
_PARAMS = [
    "moisture_pct",
    "fine_extract_db_pct",
    "wort_pH",
    "diastatic_power_WK",
    "total_protein_pct",
    "wort_colour_EBC",
]

# ---------------------------------------------------------------------------
# Supplier archetypes
# ---------------------------------------------------------------------------
# Three commercially realistic maltster styles derived from published COA
# ranges (Weyermann, Crisp, Barrett Burston, Viking, Malteurop datasheets).
# Each archetype is a mean vector over _PARAMS.
#
# Archetype A — Premium Continental (high extract, low protein, moderate DP)
#   Typical of: Weyermann Pilsner, Crisp Lager, Viking Pilsner
# Archetype B — Standard Commercial (balanced — the industry workhorse)
#   Typical of: Malteurop Pale, COFCO Pilsner, Canada Malting
# Archetype C — High Enzyme (higher protein, high DP, suits adjunct brewing)
#   Typical of: North American 2-row, BBM, high-N barley varieties
#
# Columns: moisture, extract, pH, DP, protein, EBC
_ARCHETYPES: np.ndarray = np.array(
    [
        [4.10, 82.50, 5.950, 305.0, 10.20, 3.80],  # A — Premium Continental
        [4.30, 81.80, 5.930, 335.0, 10.85, 4.00],  # B — Standard Commercial
        [4.50, 81.10, 5.910, 360.0, 11.40, 3.70],  # C — High Enzyme
    ],
    dtype=np.float64,
)

# Between-archetype jitter applied when assigning archetypes to suppliers so
# that two suppliers with the same archetype are not identical.
# Std devs: [moisture, extract, pH, DP, protein, EBC]
_ARCHETYPE_JITTER_STD: np.ndarray = np.array(
    [0.08, 0.20, 0.015, 8.0, 0.15, 0.10],
    dtype=np.float64,
)

# ---------------------------------------------------------------------------
# Lot-to-lot variation
# ---------------------------------------------------------------------------
# Commercial maltsters maintain tight process control.  These standard
# deviations represent realistic lot-to-lot variation within a single
# supplier's production run.
# Std devs: [moisture, extract, pH, DP, protein, EBC]
_LOT_STD: np.ndarray = np.array(
    [0.15, 0.35, 0.050, 12.0, 0.25, 0.15],
    dtype=np.float64,
)

# ---------------------------------------------------------------------------
# Correlation matrix (6 × 6) — malting science basis
# ---------------------------------------------------------------------------
# Parameter order: moisture, extract, pH, DP, protein, EBC
#
# Key correlations:
#   extract  ↔ protein   −0.70  (high protein = less starch = less yield)
#   protein  ↔ DP        +0.45  (high-protein grain has larger aleurone layer)
#   EBC      ↔ DP        −0.35  (higher kilning darkens malt, destroys amylase)
#   extract  ↔ DP        −0.20  (slight yield/enzyme trade-off)
#   protein  ↔ pH        −0.20  (higher protein buffers pH slightly downward)
#   moisture ↔ extract   −0.15  (wetter malt, marginally less dry-matter yield)
#   EBC      ↔ extract   +0.10  (better-modified malt: slightly higher both)
#   all others           ~0.05  (negligible)
_CORR: np.ndarray = np.array(
    [
        # moist  extr   pH     DP     prot   EBC
        [1.000, -0.150, 0.050, 0.050, 0.100, 0.050],  # moisture
        [-0.150, 1.000, 0.100, -0.200, -0.700, 0.100],  # extract
        [0.050, 0.100, 1.000, -0.100, -0.200, 0.050],  # pH
        [0.050, -0.200, -0.100, 1.000, 0.450, -0.350],  # DP
        [0.100, -0.700, -0.200, 0.450, 1.000, -0.100],  # protein
        [0.050, 0.100, 0.050, -0.350, -0.100, 1.000],  # EBC
    ],
    dtype=np.float64,
)

# ---------------------------------------------------------------------------
# Hard science bounds (EBC Analytica / ASBC) — used for clipping
# ---------------------------------------------------------------------------
# Columns: [min, max] per parameter in _PARAMS order
_SCIENCE_BOUNDS: np.ndarray = np.array(
    [
        [3.5, 6.5],    # moisture_pct
        [78.0, 86.0],  # fine_extract_db_pct
        [5.6, 6.2],    # wort_pH
        [150.0, 550.0],  # diastatic_power_WK
        [8.5, 13.5],   # total_protein_pct
        [2.5, 12.0],   # wort_colour_EBC
    ],
    dtype=np.float64,
)


# Cholesky factor of the correlation matrix — computed once at import time.
# Decomposing the correlation matrix (all unit variances) is numerically
# far more stable than decomposing the full covariance matrix, which has a
# condition number of ~60,000 due to the wide range of parameter scales
# (DP σ=12 vs moisture σ=0.15).  We scale and shift after sampling.
_CORR_L: np.ndarray = np.linalg.cholesky(_CORR)


def _sample_coa(
    rng_np: np.random.Generator,
    mean: np.ndarray,
    std: np.ndarray,
    n: int = 1,
) -> np.ndarray:
    """Draw n correlated COA vectors, clipped to science bounds.

    Algorithm:
        1. Draw z ~ N(0, I)  (independent standard normals)
        2. Apply Cholesky factor of the correlation matrix: z_corr = z @ L.T
        3. Scale and shift:  x = mean + std * z_corr
        4. Clip to EBC/ASBC hard science bounds per parameter.

    Working in correlation-matrix space avoids ill-conditioning from the
    large difference in parameter scales (DP vs moisture).
    """
    z = rng_np.standard_normal((n, len(mean)))
    # np.einsum bypasses the Apple Accelerate BLAS matmul path that emits
    # spurious "divide by zero" warnings on NumPy 2.x / macOS.
    # Equivalent to: z @ _CORR_L.T
    z_corr = np.einsum("ni,ji->nj", z, _CORR_L)
    samples = mean + std * z_corr
    for j in range(len(_PARAMS)):
        samples[:, j] = np.clip(
            samples[:, j], _SCIENCE_BOUNDS[j, 0], _SCIENCE_BOUNDS[j, 1]
        )
    return samples


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_synthetic_dataset(
    output_dir: str | Path,
    seed: int = 42,
    n_silos: int = 3,
    n_suppliers: int = 3,
    n_lots: int = 12,
) -> Path:
    """Generate a physically realistic synthetic brewery dataset.

    Improvements over the original uniform-random generator:

    1. **Correlated COA parameters** — supplier parameters are drawn from a
       multivariate normal distribution whose correlation matrix encodes known
       malting-science relationships (e.g. extract ↔ protein −0.70).

    2. **Supplier archetypes** — each supplier is assigned one of three
       commercially realistic maltster profiles (Premium Continental, Standard
       Commercial, High Enzyme) so suppliers differ meaningfully, not randomly.

    3. **Realistic lot-to-lot variation** — within a supplier, lot COA values
       vary with tight commercial process-control standard deviations rather
       than spanning the full parameter range.

    4. **Variable lot sizes** — lot masses are drawn from a truncated normal
       (mean 2000 kg, σ 400 kg, range 800–5000 kg) instead of a fixed 2000 kg.

    5. **Variable silo fill levels** — silos start at 40–100% fill instead of
       always being completely full, reflecting real mid-cycle inventory states.

    6. **Science-bound clipping** — all sampled values are clipped to EBC/ASBC
       hard limits so no physically impossible COA can appear.

    Parameters
    ----------
    output_dir : str | Path
        Directory to write the four CSV files.
    seed : int
        Random seed for full reproducibility.
    n_silos : int
        Number of silos (must be 3 for current architecture).
    n_suppliers : int
        Number of distinct suppliers (≥ 1).
    n_lots : int
        Total lots to generate (≥ n_silos × SILO_SLOT_COUNT).

    Returns
    -------
    Path
        The output directory path.
    """
    if n_silos != SILO_COUNT:
        raise ValueError(f"Synthetic generator requires exactly {SILO_COUNT} silos.")
    min_lots = SILO_COUNT * SILO_SLOT_COUNT
    if n_lots < min_lots:
        raise ValueError(f"n_lots must be >= {min_lots} for whole-lot block fill.")
    if n_suppliers < 1:
        raise ValueError("n_suppliers must be >= 1.")

    # Two independent RNGs: stdlib random (for choices/shuffles) + numpy.
    rng = random.Random(seed)
    rng_np = np.random.default_rng(seed)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Suppliers — assign archetype, jitter mean, sample representative COA
    # ------------------------------------------------------------------
    supplier_names = [f"SUP{i + 1}" for i in range(n_suppliers)]
    # Cycle through archetypes so all three styles appear when n_suppliers >= 3.
    archetype_indices = [i % len(_ARCHETYPES) for i in range(n_suppliers)]
    # Shuffle so archetype assignment isn't always A→B→C in order.
    rng.shuffle(archetype_indices)

    supplier_means: dict[str, np.ndarray] = {}
    suppliers_rows: list[dict] = []

    for name, arch_idx in zip(supplier_names, archetype_indices):
        # Add small jitter to the archetype mean → this supplier's characteristic mean.
        base = _ARCHETYPES[arch_idx]
        mean = _sample_coa(rng_np, base, _ARCHETYPE_JITTER_STD, n=1)[0]
        supplier_means[name] = mean

        # The published supplier COA is the archetype mean (what they advertise);
        # individual lots will vary around this.
        suppliers_rows.append(
            {
                "supplier": name,
                "moisture_pct": round(float(mean[0]), 3),
                "fine_extract_db_pct": round(float(mean[1]), 3),
                "wort_pH": round(float(mean[2]), 3),
                "diastatic_power_WK": round(float(mean[3]), 1),
                "total_protein_pct": round(float(mean[4]), 3),
                "wort_colour_EBC": round(float(mean[5]), 3),
            }
        )

    # ------------------------------------------------------------------
    # 2. Silos — variable geometry
    # ------------------------------------------------------------------
    silos_rows: list[dict] = []
    for i in range(n_silos):
        body_d = rng.uniform(2.8, 3.4)
        silos_rows.append(
            {
                "silo_id": f"S{i + 1}",
                "capacity_kg": float(SILO_CAPACITY_KG),
                "body_diameter_m": round(body_d, 3),
                "outlet_diameter_m": round(rng.uniform(0.18, 0.23), 3),
            }
        )

    # ------------------------------------------------------------------
    # 3. Lots — variable mass, correlated COA per supplier
    # ------------------------------------------------------------------
    lots: list[dict] = []
    for i in range(n_lots):
        supplier = rng.choice(supplier_names)
        sup_mean = supplier_means[supplier]

        # Sample this lot's COA from its supplier's distribution.
        lot_coa = _sample_coa(rng_np, sup_mean, _LOT_STD, n=1)[0]

        # Variable lot mass: truncated normal (mean 2000 kg, σ 400 kg, [800, 5000]).
        raw_mass = rng_np.normal(loc=2000.0, scale=400.0)
        lot_mass = float(np.clip(raw_mass, 800.0, 5000.0))

        lots.append(
            {
                "lot_id": f"L{1000 + i}",
                "supplier": supplier,
                "mass_kg": round(lot_mass, 1),
                "moisture_pct": round(float(lot_coa[0]), 3),
                "fine_extract_db_pct": round(float(lot_coa[1]), 3),
                "wort_pH": round(float(lot_coa[2]), 3),
                "diastatic_power_WK": round(float(lot_coa[3]), 1),
                "total_protein_pct": round(float(lot_coa[4]), 3),
                "wort_colour_EBC": round(float(lot_coa[5]), 3),
            }
        )

    # ------------------------------------------------------------------
    # 4. Layers — variable silo fill (40–100% capacity)
    # ------------------------------------------------------------------
    # Decide fill fraction per silo independently.
    silo_fill_fractions = {
        f"S{i + 1}": rng.uniform(0.40, 1.00) for i in range(n_silos)
    }

    layers_rows: list[dict] = []
    block_lots = lots[:min_lots]

    for i, lot in enumerate(block_lots):
        silo_idx = i // SILO_SLOT_COUNT
        layer_idx = (i % SILO_SLOT_COUNT) + 1
        silo_id = f"S{silo_idx + 1}"
        fill_frac = silo_fill_fractions[silo_id]

        # Scale each lot's mass so the silo lands at the target fill level.
        # Total target mass = capacity × fill_frac; split across SILO_SLOT_COUNT layers.
        target_silo_mass = SILO_CAPACITY_KG * fill_frac
        segment_mass = round(target_silo_mass / SILO_SLOT_COUNT, 1)

        layers_rows.append(
            {
                "silo_id": silo_id,
                "layer_index": layer_idx,
                "lot_id": lot["lot_id"],
                "supplier": lot["supplier"],
                "segment_mass_kg": segment_mass,
            }
        )

    # ------------------------------------------------------------------
    # 5. Discharge — mix of mass and fraction specs
    # ------------------------------------------------------------------
    discharge_rows: list[dict] = []
    for silo in silos_rows:
        if rng.random() < 0.5:
            discharge_rows.append(
                {
                    "silo_id": silo["silo_id"],
                    "discharge_mass_kg": round(rng.uniform(500.0, 1800.0), 3),
                    "discharge_fraction": "",
                }
            )
        else:
            discharge_rows.append(
                {
                    "silo_id": silo["silo_id"],
                    "discharge_mass_kg": "",
                    "discharge_fraction": round(rng.uniform(0.2, 0.7), 3),
                }
            )

    # ------------------------------------------------------------------
    # Write CSVs
    # ------------------------------------------------------------------
    _write_csv(
        out / "silos.csv",
        ["silo_id", "capacity_kg", "body_diameter_m", "outlet_diameter_m"],
        silos_rows,
    )
    _write_csv(
        out / "layers.csv",
        ["silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"],
        layers_rows,
    )
    _write_csv(
        out / "suppliers.csv",
        [
            "supplier",
            "moisture_pct",
            "fine_extract_db_pct",
            "wort_pH",
            "diastatic_power_WK",
            "total_protein_pct",
            "wort_colour_EBC",
        ],
        suppliers_rows,
    )
    _write_csv(
        out / "discharge.csv",
        ["silo_id", "discharge_mass_kg", "discharge_fraction"],
        discharge_rows,
    )
    return out
