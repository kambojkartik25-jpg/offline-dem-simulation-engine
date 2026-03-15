from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from .brew_physics import (
    janssen_effective_density,
    layer_discharge_composition,
    moisture_flow_factor,
)
from .model import BeverlooParams, Material, run_multi_silo_blend


@dataclass(frozen=True)
class RunConfig:
    rho_bulk_kg_m3: float = 610.0
    grain_diameter_m: float = 0.004
    beverloo_c: float = 0.58
    beverloo_k: float = 1.4
    gravity_m_s2: float = 9.81
    sigma_m: float = 0.12
    steps: int = 2000
    auto_adjust: bool = True
    silo_radius_m: float = 1.5
    mu_wall: float = 0.4
    use_janssen: bool = True
    use_moisture_correction: bool = True
    use_layer_composition: bool = True
    arch_prob: float = 0.05
    arch_duration_s: float = 2.0


def run_blend(inputs: Dict[str, Any], cfg: RunConfig) -> Dict[str, Any]:
    layers_df = inputs["layers"].copy()
    silos_df = inputs["silos"].copy()
    suppliers_df = inputs["suppliers"].copy()

    physics_corrections_applied: list[str] = []
    flow_multiplier_by_silo: dict[str, float] = {}
    janssen_applied = False
    moisture_applied = False

    # ── BrewPhysics: Janssen correction ──
    if cfg.use_janssen and cfg.rho_bulk_kg_m3 > 0 and cfg.silo_radius_m > 0:
        area = 3.141592653589793 * (float(cfg.silo_radius_m) ** 2)
        for _, silo_row in silos_df.iterrows():
            sid = str(silo_row.get("silo_id", ""))
            if not sid:
                continue
            layer_rows = layers_df[layers_df["silo_id"].astype(str) == sid]
            if "segment_mass_kg" not in layer_rows.columns:
                continue
            mass_kg = float(
                pd.to_numeric(layer_rows["segment_mass_kg"], errors="coerce").fillna(0.0).sum()
            )
            if mass_kg <= 0:
                continue
            fill_height = mass_kg / (float(cfg.rho_bulk_kg_m3) * area) if area > 0 else 0.0
            rho_eff = janssen_effective_density(
                rho_bulk=float(cfg.rho_bulk_kg_m3),
                fill_height=fill_height,
                silo_radius=float(cfg.silo_radius_m),
                mu_wall=float(cfg.mu_wall),
            )
            ratio = (rho_eff / float(cfg.rho_bulk_kg_m3)) if cfg.rho_bulk_kg_m3 > 0 else 1.0
            flow_multiplier_by_silo[sid] = max(0.0, float(ratio))
            janssen_applied = True

    # ── BrewPhysics: Moisture correction ──
    if cfg.use_moisture_correction and "moisture_pct" in layers_df.columns:
        for _, silo_row in silos_df.iterrows():
            sid = str(silo_row.get("silo_id", ""))
            if not sid:
                continue
            layer_rows = layers_df[layers_df["silo_id"].astype(str) == sid]
            if layer_rows.empty:
                continue
            moisture = float(pd.to_numeric(layer_rows["moisture_pct"], errors="coerce").dropna().mean())
            if moisture != moisture:
                continue
            factor = moisture_flow_factor(moisture)
            flow_multiplier_by_silo[sid] = flow_multiplier_by_silo.get(sid, 1.0) * float(factor)
            moisture_applied = True

    material = Material(
        rho_bulk_kg_m3=cfg.rho_bulk_kg_m3,
        grain_diameter_m=cfg.grain_diameter_m,
    )
    bev = BeverlooParams(
        C=cfg.beverloo_c,
        k=cfg.beverloo_k,
        g_m_s2=cfg.gravity_m_s2,
    )
    result = run_multi_silo_blend(
        df_silos=inputs["silos"],
        df_layers=inputs["layers"],
        df_suppliers=inputs["suppliers"],
        df_discharge=inputs["discharge"],
        material=material,
        bev=bev,
        sigma_m=cfg.sigma_m,
        steps=cfg.steps,
        auto_adjust=cfg.auto_adjust,
    )

    # ── BrewPhysics: Janssen correction ──
    # ── BrewPhysics: Moisture correction ──
    if flow_multiplier_by_silo:
        for sid, silo_res in result.get("per_silo", {}).items():
            mult = float(flow_multiplier_by_silo.get(str(sid), 1.0))
            base_rate = float(silo_res.get("mass_flow_rate_kg_s", 0.0))
            if base_rate > 0:
                new_rate = max(0.0, base_rate * mult)
                silo_res["mass_flow_rate_kg_s"] = new_rate
                discharge_mass = float(silo_res.get("discharged_mass_kg", 0.0))
                silo_res["discharge_time_s"] = (discharge_mass / new_rate) if new_rate > 0 else 0.0

    # ── BrewPhysics: Layer composition ──
    layer_composition_used = False
    if cfg.use_layer_composition:
        layer_inputs = layers_df.copy()
        if "layer_position" not in layer_inputs.columns and "layer_index" in layer_inputs.columns:
            layer_inputs["layer_position"] = pd.to_numeric(
                layer_inputs["layer_index"], errors="coerce"
            ).fillna(0.0)
        param_keys = [c for c in suppliers_df.columns if c != "supplier"]
        if param_keys:
            for p in param_keys:
                if p not in layer_inputs.columns and p in suppliers_df.columns:
                    supplier_map = suppliers_df.set_index("supplier")[p].to_dict()
                    layer_inputs[p] = layer_inputs["supplier"].astype(str).map(supplier_map)

            weighted_values = {k: 0.0 for k in param_keys}
            total_mass = 0.0
            for sid, silo_res in result.get("per_silo", {}).items():
                discharge_mass = float(silo_res.get("discharged_mass_kg", 0.0) or 0.0)
                if discharge_mass <= 0:
                    continue
                comp = layer_discharge_composition(
                    layers_df=layer_inputs,
                    silo_id=str(sid),
                    discharged_mass_kg=discharge_mass,
                    param_keys=param_keys,
                )
                if not comp:
                    comp = {
                        k: float(v)
                        for k, v in (silo_res.get("blended_params_per_silo", {}) or {}).items()
                        if k in param_keys
                    }
                else:
                    layer_composition_used = True
                if not comp:
                    continue
                for k in param_keys:
                    if k in comp:
                        weighted_values[k] += discharge_mass * float(comp[k])
                total_mass += discharge_mass
            if total_mass > 0:
                result["total_blended_params"] = {
                    k: float(weighted_values[k] / total_mass)
                    for k in param_keys
                    if weighted_values[k] == weighted_values[k]
                }

    if janssen_applied:
        physics_corrections_applied.append("janssen_stress_correction")
    if moisture_applied:
        physics_corrections_applied.append("moisture_flow_factor")
    if layer_composition_used:
        physics_corrections_applied.append("layer_discharge_composition")
    result["physics_corrections_applied"] = physics_corrections_applied
    return result
