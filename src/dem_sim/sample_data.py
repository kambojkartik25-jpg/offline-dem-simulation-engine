from __future__ import annotations

from pathlib import Path

LOT_SIZE_KG = 2000
SILO_SLOT_COUNT = 4
SILO_COUNT = 3
SILO_CAPACITY_KG = LOT_SIZE_KG * SILO_SLOT_COUNT

SILOS_CSV = """silo_id,capacity_kg,body_diameter_m,outlet_diameter_m
S1,8000,3.0,0.20
S2,8000,3.2,0.20
S3,8000,3.1,0.21
"""

LAYERS_CSV = """silo_id,layer_index,lot_id,supplier,segment_mass_kg
S1,1,L1001,BBM,2000
S1,2,L1002,COFCO,2000
S1,3,L1003,Malteurop,2000
S1,4,L1004,BBM,2000
S2,1,L1005,COFCO,2000
S2,2,L1006,Malteurop,2000
S2,3,L1007,BBM,2000
S2,4,L1008,COFCO,2000
S3,1,L1009,Malteurop,2000
S3,2,L1010,BBM,2000
S3,3,L1011,COFCO,2000
S3,4,L1012,Malteurop,2000
"""

SUPPLIERS_CSV = """supplier,moisture_pct,fine_extract_db_pct,wort_pH,diastatic_power_WK,total_protein_pct,wort_colour_EBC
BBM,4.2,82.0,5.98,342.1,10.12,3.8
COFCO,4.4,81.8,5.93,317.4,11.1,4.0
Malteurop,4.3,81.2,5.97,336.9,10.5,3.8
"""

DISCHARGE_CSV = """silo_id,discharge_mass_kg,discharge_fraction
S1,1600,
S2,,0.5
S3,800,
"""


def write_sample_data(output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "silos.csv").write_text(SILOS_CSV, encoding="utf-8")
    (out / "layers.csv").write_text(LAYERS_CSV, encoding="utf-8")
    (out / "suppliers.csv").write_text(SUPPLIERS_CSV, encoding="utf-8")
    (out / "discharge.csv").write_text(DISCHARGE_CSV, encoding="utf-8")
