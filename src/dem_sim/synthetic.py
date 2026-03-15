from __future__ import annotations

import csv
import random
from pathlib import Path

LOT_SIZE_KG = 2000
SILO_SLOT_COUNT = 4
SILO_COUNT = 3
SILO_CAPACITY_KG = LOT_SIZE_KG * SILO_SLOT_COUNT


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
    if n_silos != SILO_COUNT:
        raise ValueError(f"Synthetic generator requires exactly {SILO_COUNT} silos.")
    min_lots = SILO_COUNT * SILO_SLOT_COUNT
    if n_lots < min_lots:
        raise ValueError(f"n_lots must be >= {min_lots} for whole-lot block fill.")
    rng = random.Random(seed)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    supplier_names = [f"SUP{i+1}" for i in range(n_suppliers)]

    suppliers = []
    for supplier in supplier_names:
        suppliers.append(
            {
                "supplier": supplier,
                "moisture_pct": round(rng.uniform(3.6, 4.95), 3),          # < 5.0
                "fine_extract_db_pct": round(rng.uniform(81.05, 83.0), 3), # > 81.0
                "wort_pH": round(rng.uniform(5.8, 6.0), 3),                # 5.8 - 6.0
                "diastatic_power_WK": round(rng.uniform(300.1, 360.0), 3), # > 300
                "total_protein_pct": round(rng.uniform(10.2, 11.2), 3),    # 10.2 - 11.2
                "wort_colour_EBC": round(rng.uniform(4.3, 4.7), 3),        # 4.3 - 4.7
            }
        )

    silos = []
    for i in range(n_silos):
        body_d = rng.uniform(2.8, 3.4)
        silos.append(
            {
                "silo_id": f"S{i+1}",
                "capacity_kg": float(SILO_CAPACITY_KG),
                "body_diameter_m": round(body_d, 3),
                "outlet_diameter_m": round(rng.uniform(0.18, 0.23), 3),
            }
        )

    lots = []
    for i in range(n_lots):
        lots.append(
            {
                "lot_id": f"L{1000+i}",
                "supplier": rng.choice(supplier_names),
                "mass_kg": float(LOT_SIZE_KG),
            }
        )

    layers = []
    block_lots = lots[:min_lots]
    for i, lot in enumerate(block_lots):
        silo_idx = i // SILO_SLOT_COUNT
        layer_idx = (i % SILO_SLOT_COUNT) + 1
        layers.append(
            {
                "silo_id": f"S{silo_idx + 1}",
                "layer_index": layer_idx,
                "lot_id": lot["lot_id"],
                "supplier": lot["supplier"],
                "segment_mass_kg": float(LOT_SIZE_KG),
            }
        )

    discharge = []
    for silo in silos:
        if rng.random() < 0.5:
            discharge.append(
                {
                    "silo_id": silo["silo_id"],
                    "discharge_mass_kg": round(rng.uniform(500.0, 1800.0), 3),
                    "discharge_fraction": "",
                }
            )
        else:
            discharge.append(
                {
                    "silo_id": silo["silo_id"],
                    "discharge_mass_kg": "",
                    "discharge_fraction": round(rng.uniform(0.2, 0.7), 3),
                }
            )

    _write_csv(
        out / "silos.csv",
        ["silo_id", "capacity_kg", "body_diameter_m", "outlet_diameter_m"],
        silos,
    )
    _write_csv(
        out / "layers.csv",
        ["silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"],
        layers,
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
        suppliers,
    )
    _write_csv(
        out / "discharge.csv",
        ["silo_id", "discharge_mass_kg", "discharge_fraction"],
        discharge,
    )
    return out
