from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IntermittentFlowResult:
    mean_rate_kg_s: float
    std_rate_kg_s: float
    p10_rate_kg_s: float
    p90_rate_kg_s: float


def janssen_effective_density(
    rho_bulk: float,
    fill_height: float,
    silo_radius: float,
    mu_wall: float = 0.4,
    k_ratio: float = 0.4,
) -> float:
    """
    Corrects bulk density at the silo orifice for wall friction stress
    redistribution using the Janssen equation.

    For tall silos, wall friction means the vertical stress at the base
    is significantly less than rho * g * H. Beverloo overestimates flow
    rate from full silos without this correction.

    Args:
        rho_bulk:      Nominal bulk density of malt (kg/m3)
        fill_height:   Current fill height of silo (m)
        silo_radius:   Internal silo radius (m)
        mu_wall:       Wall friction coefficient (default 0.4 for steel/malt)
        k_ratio:       Lateral/vertical stress ratio (default 0.4)

    Returns:
        Effective bulk density at orifice (kg/m3), clipped to [0.3*rho, rho]
    """
    rho_bulk = float(rho_bulk)
    fill_height = float(fill_height)
    silo_radius = float(silo_radius)
    if rho_bulk <= 0 or silo_radius <= 0:
        return rho_bulk
    if fill_height <= 0:
        return rho_bulk
    area = np.pi * (silo_radius**2)
    perimeter = 2.0 * np.pi * silo_radius
    lambda_j = float(mu_wall) * float(k_ratio) * perimeter / area
    if lambda_j <= 0:
        return rho_bulk
    sigma_v = (rho_bulk * 9.81 / lambda_j) * (1.0 - np.exp(-lambda_j * fill_height))
    rho_eff = sigma_v / (9.81 * fill_height) if fill_height > 0 else rho_bulk
    return float(np.clip(rho_eff, 0.3 * rho_bulk, rho_bulk))


def moisture_flow_factor(
    moisture_pct: float,
    m0: float = 12.0,
    k: float = 0.08,
) -> float:
    """
    Returns a flow rate multiplier [0.5, 1.0] based on malt moisture content.

    Above ~12% moisture, inter-particle adhesion increases flow resistance
    non-linearly. This factor multiplies the Beverloo base flow rate.

    Args:
        moisture_pct:  Moisture content as a percentage (e.g. 14.5)
        m0:            Reference moisture threshold (default 12.0%)
        k:             Sensitivity coefficient (default 0.08)

    Returns:
        Float multiplier between 0.5 and 1.0
    """
    moisture_pct = float(moisture_pct)
    if moisture_pct <= float(m0):
        return 1.0
    return float(np.clip(np.exp(-float(k) * (moisture_pct - float(m0))), 0.5, 1.0))


def beverloo_intermittent(
    q_beverloo: float,
    arch_prob: float = 0.05,
    arch_duration_s: float = 2.0,
    n_sim: int = 2000,
    duration_s: float = 300.0,
) -> IntermittentFlowResult:
    """
    Models discharge as an alternating run/stall renewal process to capture
    flow intermittency from arching events in the silo.

    Instead of a single deterministic flow rate, returns P10/P50/P90
    quantiles over n_sim Monte Carlo simulations.

    Args:
        q_beverloo:       Base Beverloo flow rate (kg/s)
        arch_prob:        Probability of arch forming per second (default 0.05)
        arch_duration_s:  Mean time to clear an arch in seconds (default 2.0)
        n_sim:            Number of Monte Carlo simulations (default 2000)
        duration_s:       Simulated discharge window in seconds (default 300.0)

    Returns:
        IntermittentFlowResult dataclass with fields:
            mean_rate_kg_s, std_rate_kg_s, p10_rate_kg_s, p90_rate_kg_s
    """
    q_beverloo = max(0.0, float(q_beverloo))
    arch_prob = max(1e-9, float(arch_prob))
    arch_duration_s = max(1e-9, float(arch_duration_s))
    n_sim = max(1, int(n_sim))
    duration_s = max(1e-9, float(duration_s))
    rng = np.random.default_rng(42)
    rates = np.zeros(n_sim, dtype=float)
    run_scale = 1.0 / arch_prob
    stall_scale = arch_duration_s

    for i in range(n_sim):
        t = 0.0
        flowed_mass = 0.0
        flowing = True
        while t < duration_s:
            seg = float(rng.exponential(run_scale if flowing else stall_scale))
            seg_eff = min(seg, duration_s - t)
            if flowing:
                flowed_mass += q_beverloo * seg_eff
            t += seg_eff
            flowing = not flowing
        rates[i] = flowed_mass / duration_s

    return IntermittentFlowResult(
        mean_rate_kg_s=float(np.mean(rates)),
        std_rate_kg_s=float(np.std(rates)),
        p10_rate_kg_s=float(np.quantile(rates, 0.10)),
        p90_rate_kg_s=float(np.quantile(rates, 0.90)),
    )


def layer_discharge_composition(
    layers_df: pd.DataFrame,
    silo_id: str,
    discharged_mass_kg: float,
    param_keys: list[str],
) -> dict[str, float]:
    """
    Computes mass-weighted blend composition accounting for layer
    stratification and FIFO discharge order (bottom layers first).

    This corrects the assumption in run_blend that discharging X kg from
    a silo yields a uniform average of all layers. In reality, bottom
    layers discharge first and may have different moisture/protein/extract
    due to settling and segregation.

    Args:
        layers_df:           DataFrame with silo layer data
        silo_id:             The silo to compute composition for
        discharged_mass_kg:  How much mass is being discharged
        param_keys:          List of parameter column names to average

    Returns:
        dict mapping param_key -> mass-weighted average value
        Returns empty dict if silo_id not found or no layers available.
    """
    if layers_df is None or layers_df.empty:
        return {}
    if "silo_id" not in layers_df.columns or "layer_position" not in layers_df.columns:
        return {}
    if "segment_mass_kg" not in layers_df.columns:
        return {}
    d = layers_df[layers_df["silo_id"].astype(str) == str(silo_id)].copy()
    if d.empty:
        return {}
    d = d.sort_values("layer_position", kind="mergesort")
    remaining = max(0.0, float(discharged_mass_kg))
    if remaining <= 0:
        return {}
    taken_rows: list[tuple[float, dict[str, float]]] = []
    for _, row in d.iterrows():
        layer_mass = max(0.0, float(row.get("segment_mass_kg", 0.0) or 0.0))
        if layer_mass <= 0:
            continue
        take = min(layer_mass, remaining)
        if take <= 0:
            continue
        vals: dict[str, float] = {}
        for key in param_keys:
            if key in row.index and pd.notna(row[key]):
                vals[key] = float(row[key])
        taken_rows.append((take, vals))
        remaining -= take
        if remaining <= 1e-12:
            break
    if not taken_rows:
        return {}
    total_taken = float(sum(m for m, _ in taken_rows))
    if total_taken <= 0:
        return {}
    out: dict[str, float] = {}
    for key in param_keys:
        weighted = [(m, vals[key]) for m, vals in taken_rows if key in vals]
        if not weighted:
            continue
        out[key] = float(sum(m * v for m, v in weighted) / sum(m for m, _ in weighted))
    return out
