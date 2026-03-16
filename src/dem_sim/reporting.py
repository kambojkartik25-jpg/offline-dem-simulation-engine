from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def _jsonable_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, (float, int, str, bool)) or v is None:
            out[k] = v
        else:
            out[k] = str(v)
    return out


def write_outputs(result: Dict[str, Any], output_dir: str | Path) -> Dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    segment_csv = out / "segment_contributions.csv"
    lot_csv = out / "lot_contributions.csv"
    silo_state_ledger_csv = out / "silo_state_ledger.csv"
    lot_state_ledger_csv = out / "lot_state_ledger.csv"
    segment_state_ledger_csv = out / "segment_state_ledger.csv"
    summary_json = out / "summary.json"

    result["df_segment_contrib_all"].to_csv(segment_csv, index=False)
    result["df_lot_contrib_all"].to_csv(lot_csv, index=False)
    result["df_silo_state_ledger"].to_csv(silo_state_ledger_csv, index=False)
    result["df_lot_state_ledger"].to_csv(lot_state_ledger_csv, index=False)
    result["df_segment_state_ledger"].to_csv(segment_state_ledger_csv, index=False)

    per_silo_summary = {}
    for silo_id, silo_result in result["per_silo"].items():
        per_silo_summary[silo_id] = {
            "discharged_mass_kg": float(silo_result["discharged_mass_kg"]),
            "mass_flow_rate_kg_s": float(silo_result["mass_flow_rate_kg_s"]),
            "discharge_time_s": float(silo_result["discharge_time_s"]),
            "sigma_m": float(silo_result["sigma_m"]),
            "blended_params_per_silo": _jsonable_dict(
                silo_result["blended_params_per_silo"]
            ),
        }

    payload = {
        "total_discharged_mass_kg": float(result["total_discharged_mass_kg"]),
        "total_remaining_mass_kg": float(result["total_remaining_mass_kg"]),
        "total_blended_params": _jsonable_dict(result["total_blended_params"]),
        "per_silo": per_silo_summary,
    }
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return {
        "segment_contributions_csv": segment_csv,
        "lot_contributions_csv": lot_csv,
        "silo_state_ledger_csv": silo_state_ledger_csv,
        "lot_state_ledger_csv": lot_state_ledger_csv,
        "segment_state_ledger_csv": segment_state_ledger_csv,
        "summary_json": summary_json,
    }


def terminal_summary(result: Dict[str, Any]) -> str:
    lines = []
    lines.append("Simulation complete")
    lines.append(f"Total discharged mass: {result['total_discharged_mass_kg']:.3f} kg")
    lines.append(f"Total remaining mass: {result['total_remaining_mass_kg']:.3f} kg")
    lines.append("Total blended parameters:")
    for k, v in result["total_blended_params"].items():
        lines.append(f"- {k}: {v:.4f}")

    lines.append("Per-silo:")
    for silo_id, silo_result in result["per_silo"].items():
        lines.append(
            f"- {silo_id}: discharge={silo_result['discharged_mass_kg']:.3f} kg, "
            f"flow={silo_result['mass_flow_rate_kg_s']:.3f} kg/s, "
            f"time={silo_result['discharge_time_s']:.3f} s, sigma={silo_result['sigma_m']:.4f} m"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Supplier COA reference bounds (EBC Analytica + ASBC Methods of Analysis)
# ---------------------------------------------------------------------------

# Hard physical limits — values outside these are scientifically impossible
# for commercial base malt and indicate a data entry error or unit mismatch.
SUPPLIER_COA_SCIENCE_BOUNDS: dict[str, tuple[float, float]] = {
    "moisture_pct":        (3.5,  6.5),   # EBC 4.2: kiln floor min / storage max
    "fine_extract_db_pct": (78.0, 86.0),  # EBC 4.5.1: minimum conversion / 2-row ceiling
    "wort_pH":             (5.6,  6.2),   # ASBC Malt-6: buffering min / under-modification max
    "diastatic_power_WK":  (150.0, 550.0),# EBC 4.12: self-conversion min / specialty strain max
    "total_protein_pct":   (8.5,  13.5),  # EBC 4.3.1: nitrogen-deficient min / haze-risk max
    "wort_colour_EBC":     (2.5,  12.0),  # EBC 8.5: any-kilned-malt floor / pre-crystal ceiling
}

# Advisory typical contract range — unusual but physically possible; brewer should review.
SUPPLIER_COA_TYPICAL_RANGE: dict[str, tuple[float, float]] = {
    "moisture_pct":        (3.8,  5.5),
    "fine_extract_db_pct": (80.0, 84.5),
    "wort_pH":             (5.7,  6.1),
    "diastatic_power_WK":  (200.0, 450.0),
    "total_protein_pct":   (9.5,  12.5),
    "wort_colour_EBC":     (3.0,  8.0),
}

# Human-readable parameter labels for error messages.
_COA_PARAM_LABELS: dict[str, str] = {
    "moisture_pct":        "Moisture %",
    "fine_extract_db_pct": "Fine Extract db%",
    "wort_pH":             "Wort pH",
    "diastatic_power_WK":  "Diastatic Power (WK)",
    "total_protein_pct":   "Total Protein %",
    "wort_colour_EBC":     "Wort Colour (EBC)",
}


def validate_supplier_coa(
    suppliers_df: pd.DataFrame,
) -> tuple[list[str], list[str]]:
    """Validate supplier COA parameters against science bounds and typical contract ranges.

    Returns
    -------
    errors : list[str]
        Values outside physical science bounds (EBC/ASBC). These block the request.
    warnings : list[str]
        Values inside science bounds but outside the typical contract range. These
        are advisory — simulation runs, but the brewer should review.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if suppliers_df.empty:
        return errors, warnings

    for _, row in suppliers_df.iterrows():
        supplier = str(row.get("supplier", "unknown"))
        for param, (lo, hi) in SUPPLIER_COA_SCIENCE_BOUNDS.items():
            if param not in row.index or pd.isna(row[param]):
                continue
            val = float(row[param])
            label = _COA_PARAM_LABELS.get(param, param)
            if val < lo or val > hi:
                errors.append(
                    f"suppliers.csv COA error: supplier '{supplier}' — "
                    f"{label} value {val:.4g} is outside physical science bounds "
                    f"[{lo}–{hi}] (EBC/ASBC reference). Check units or data entry."
                )
            elif param in SUPPLIER_COA_TYPICAL_RANGE:
                warn_lo, warn_hi = SUPPLIER_COA_TYPICAL_RANGE[param]
                if val < warn_lo or val > warn_hi:
                    warnings.append(
                        f"suppliers.csv COA warning: supplier '{supplier}' — "
                        f"{label} value {val:.4g} is outside typical contract range "
                        f"[{warn_lo}–{warn_hi}] but within science bounds [{lo}–{hi}]. "
                        f"Verify COA document."
                    )

    return errors, warnings


def validate_inputs_shape(inputs: Dict[str, pd.DataFrame]) -> list[str]:
    errors: list[str] = []

    silos = inputs["silos"]
    layers = inputs["layers"]
    suppliers = inputs["suppliers"]
    discharge = inputs["discharge"]

    required_silos = {"silo_id", "capacity_kg", "body_diameter_m", "outlet_diameter_m"}
    required_layers = {"silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"}
    required_suppliers = {"supplier"}
    required_discharge = {"silo_id"}

    if not required_silos.issubset(silos.columns):
        errors.append(f"silos.csv missing: {required_silos - set(silos.columns)}")
    if not required_layers.issubset(layers.columns):
        errors.append(f"layers.csv missing: {required_layers - set(layers.columns)}")
    if not required_suppliers.issubset(suppliers.columns):
        errors.append(f"suppliers.csv missing: {required_suppliers - set(suppliers.columns)}")
    if not required_discharge.issubset(discharge.columns):
        errors.append(f"discharge.csv missing: {required_discharge - set(discharge.columns)}")

    if "silo_id" in silos.columns and silos["silo_id"].astype(str).duplicated().any():
        errors.append("silos.csv has duplicate silo_id values.")

    if "supplier" in layers.columns and "supplier" in suppliers.columns:
        unknown = set(layers["supplier"].astype(str)) - set(suppliers["supplier"].astype(str))
        if unknown:
            errors.append(f"layers.csv references unknown suppliers: {sorted(unknown)}")

    if {"capacity_kg", "body_diameter_m", "outlet_diameter_m"}.issubset(silos.columns):
        if (pd.to_numeric(silos["capacity_kg"], errors="coerce") <= 0).any():
            errors.append("silos.csv must have capacity_kg > 0 for all rows.")
        if (pd.to_numeric(silos["body_diameter_m"], errors="coerce") <= 0).any():
            errors.append("silos.csv must have body_diameter_m > 0 for all rows.")
        if (pd.to_numeric(silos["outlet_diameter_m"], errors="coerce") <= 0).any():
            errors.append("silos.csv must have outlet_diameter_m > 0 for all rows.")

    if "initial_mass_kg" in silos.columns:
        if (pd.to_numeric(silos["initial_mass_kg"], errors="coerce") < 0).any():
            errors.append("silos.csv must have initial_mass_kg >= 0 for all rows.")

    if "segment_mass_kg" in layers.columns:
        if (pd.to_numeric(layers["segment_mass_kg"], errors="coerce") < 0).any():
            errors.append("layers.csv must have segment_mass_kg >= 0 for all rows.")

    if {"silo_id", "layer_index"}.issubset(layers.columns):
        dup_layers = layers.assign(
            silo_id=layers["silo_id"].astype(str),
            layer_index=layers["layer_index"],
        ).duplicated(subset=["silo_id", "layer_index"], keep=False)
        if dup_layers.any():
            errors.append("layers.csv has duplicate (silo_id, layer_index) values.")

    if "discharge_mass_kg" in discharge.columns:
        discharge_mass = pd.to_numeric(discharge["discharge_mass_kg"], errors="coerce")
        mask = discharge["discharge_mass_kg"].notna()
        if ((discharge_mass < 0) & mask).any():
            errors.append("discharge.csv must have discharge_mass_kg >= 0 when provided.")

    if "discharge_fraction" in discharge.columns:
        frac = pd.to_numeric(discharge["discharge_fraction"], errors="coerce")
        mask = discharge["discharge_fraction"].notna()
        if (((frac < 0) | (frac > 1)) & mask).any():
            errors.append(
                "discharge.csv must have discharge_fraction between 0 and 1 when provided."
            )

    return errors
