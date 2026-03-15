"""Tests for the three brewery malt physics improvements:
  1. Moisture-dependent cohesion on Beverloo flow rate (moisture_beta)
  2. Sigma height-scaling as silo empties (sigma_alpha)
  3. Asymmetric skew-normal mixing kernel (skew_alpha)
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dem_sim.model import (
    Material,
    Silo,
    _normal_cdf_array,
    _simulate_for_sigma,
    _skew_tilt,
    layer_probabilities,
    run_multi_silo_blend,
    BeverlooParams,
)
from dem_sim.io import load_inputs
from dem_sim.sample_data import write_sample_data
from dem_sim.service import RunConfig, run_blend


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def silo() -> Silo:
    return Silo(
        silo_id="S1",
        capacity_kg=5000.0,
        body_diameter_m=3.0,
        outlet_diameter_m=0.2,
    )


@pytest.fixture()
def material() -> Material:
    return Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004)


@pytest.fixture()
def intervals() -> pd.DataFrame:
    """Four-layer silo: bottom = low moisture, top = high moisture."""
    return pd.DataFrame(
        {
            "silo_id": ["S1"] * 4,
            "layer_index": [1, 2, 3, 4],
            "lot_id": ["L1", "L2", "L3", "L4"],
            "supplier": ["LowMoist", "LowMoist", "HighMoist", "HighMoist"],
            "segment_mass_kg": [800.0, 800.0, 800.0, 800.0],
            "z0_m": [0.0, 0.25, 0.50, 0.75],
            "z1_m": [0.25, 0.50, 0.75, 1.00],
        }
    )


@pytest.fixture()
def base_kwargs(silo, material, intervals) -> dict:
    return dict(
        silo=silo,
        intervals_df=intervals,
        total_height_m=1.0,
        discharge_mass_kg=1600.0,
        m_dot_kg_s=110.0,
        material=material,
        sigma_m=0.12,
        steps=500,
    )


@pytest.fixture()
def layer_moisture_arr() -> np.ndarray:
    """Matches the intervals fixture: layers 1-2 = 4.0%, layers 3-4 = 8.0%."""
    return np.array([4.0, 4.0, 8.0, 8.0], dtype=float)


# ---------------------------------------------------------------------------
# Feature 1: Moisture-dependent cohesion
# ---------------------------------------------------------------------------

class TestMoistureBeta:
    def test_zero_beta_is_identity_no_moisture_array(self, base_kwargs):
        """beta=0 with no moisture array must give identical result to baseline."""
        result_a = _simulate_for_sigma(**base_kwargs)
        result_b = _simulate_for_sigma(**base_kwargs, moisture_beta=0.0, layer_moisture=None)
        np.testing.assert_array_almost_equal(
            result_a["discharged_mass_kg"].values,
            result_b["discharged_mass_kg"].values,
            decimal=10,
        )

    def test_zero_beta_is_identity_with_moisture_array(self, base_kwargs, layer_moisture_arr):
        """beta=0 with a moisture array must still give the same result (exp(0)=1)."""
        result_a = _simulate_for_sigma(**base_kwargs)
        result_b = _simulate_for_sigma(
            **base_kwargs, moisture_beta=0.0, layer_moisture=layer_moisture_arr
        )
        np.testing.assert_array_almost_equal(
            result_a["discharged_mass_kg"].values,
            result_b["discharged_mass_kg"].values,
            decimal=10,
        )

    def test_moisture_beta_changes_layer_distribution(self, base_kwargs, layer_moisture_arr):
        """With beta>0 the shape of discharged mass across layers must differ from beta=0."""
        result_base = _simulate_for_sigma(**base_kwargs)
        result_moist = _simulate_for_sigma(
            **base_kwargs, moisture_beta=0.05, layer_moisture=layer_moisture_arr
        )
        # Total mass is conserved by normalization in both cases.
        assert float(result_moist["discharged_mass_kg"].sum()) == pytest.approx(
            float(result_base["discharged_mass_kg"].sum()), rel=1e-6
        )
        # But the per-layer fractions must differ.
        assert not np.allclose(
            result_moist["discharged_mass_kg"].values,
            result_base["discharged_mass_kg"].values,
        )

    def test_high_moisture_layers_contribute_less_at_discharge_front(
        self, base_kwargs, layer_moisture_arr
    ):
        """High-moisture top layers should contribute proportionally less than with beta=0,
        because their effective dm is smaller when the front is in their zone."""
        result_base = _simulate_for_sigma(**base_kwargs)
        result_moist = _simulate_for_sigma(
            **base_kwargs, moisture_beta=0.05, layer_moisture=layer_moisture_arr
        )
        # Layers 3 & 4 (indices 2, 3) are HighMoist (8%). After normalization,
        # their combined fraction should be smaller with beta>0 than without.
        base_top_frac = result_base["discharged_mass_kg"].iloc[2:].sum() / result_base["discharged_mass_kg"].sum()
        moist_top_frac = result_moist["discharged_mass_kg"].iloc[2:].sum() / result_moist["discharged_mass_kg"].sum()
        assert moist_top_frac < base_top_frac

    def test_mass_conservation_with_moisture_beta(self, base_kwargs, layer_moisture_arr):
        """Total discharged mass must equal the target regardless of moisture_beta."""
        result = _simulate_for_sigma(
            **base_kwargs, moisture_beta=0.1, layer_moisture=layer_moisture_arr
        )
        assert float(result["discharged_mass_kg"].sum()) == pytest.approx(
            base_kwargs["discharge_mass_kg"], rel=1e-6
        )


# ---------------------------------------------------------------------------
# Feature 2: Sigma height-scaling
# ---------------------------------------------------------------------------

class TestSigmaAlpha:
    def test_zero_alpha_is_identity(self, base_kwargs):
        """sigma_alpha=0 must produce the same result as no scaling."""
        result_a = _simulate_for_sigma(**base_kwargs)
        result_b = _simulate_for_sigma(**base_kwargs, sigma_alpha=0.0)
        np.testing.assert_array_almost_equal(
            result_a["discharged_mass_kg"].values,
            result_b["discharged_mass_kg"].values,
            decimal=10,
        )

    def test_sigma_alpha_changes_distribution(self, base_kwargs):
        """sigma_alpha>0 must produce a different layer distribution than alpha=0."""
        result_flat = _simulate_for_sigma(**base_kwargs)
        result_scaled = _simulate_for_sigma(**base_kwargs, sigma_alpha=0.4)
        assert not np.allclose(
            result_flat["discharged_mass_kg"].values,
            result_scaled["discharged_mass_kg"].values,
        )

    def test_mass_conservation_with_sigma_alpha(self, base_kwargs):
        """Total discharged mass must be conserved with sigma scaling active."""
        result = _simulate_for_sigma(**base_kwargs, sigma_alpha=0.4)
        assert float(result["discharged_mass_kg"].sum()) == pytest.approx(
            base_kwargs["discharge_mass_kg"], rel=1e-6
        )

    def test_high_sigma_alpha_concentrates_distribution(self, base_kwargs):
        """With large alpha, the discharged mass distribution should be more concentrated
        (higher Herfindahl index) than with flat sigma, because smaller sigma means
        each timestep draws more from the layer directly at the front."""
        kw = {**base_kwargs, "discharge_mass_kg": 3000.0}
        result_flat = _simulate_for_sigma(**kw, sigma_alpha=0.0)
        result_scaled = _simulate_for_sigma(**kw, sigma_alpha=0.8)

        def hhi(series: pd.Series) -> float:
            fracs = series / series.sum()
            return float((fracs ** 2).sum())

        assert hhi(result_scaled["discharged_mass_kg"]) > hhi(result_flat["discharged_mass_kg"])


# ---------------------------------------------------------------------------
# Feature 3: Asymmetric skew-normal kernel
# ---------------------------------------------------------------------------

class TestSkewAlpha:
    def test_skew_tilt_zero_alpha_is_uniform(self):
        """_skew_tilt with alpha=0 must produce all-ones weights (no-op)."""
        z_centers = np.array([0.1, 0.3, 0.5, 0.7, 0.9])
        tilt = _skew_tilt(z_centers, z_front=0.5, sigma=0.12, alpha_skew=0.0)
        np.testing.assert_array_almost_equal(tilt, np.ones(5), decimal=12)

    def test_skew_tilt_always_positive(self):
        """_skew_tilt must always produce strictly positive weights."""
        z_centers = np.linspace(0, 1, 50)
        for alpha in [-5.0, -2.0, 0.0, 2.0, 5.0]:
            tilt = _skew_tilt(z_centers, z_front=0.5, sigma=0.12, alpha_skew=alpha)
            assert np.all(tilt > 0), f"Non-positive tilt at alpha={alpha}"

    def test_layer_probabilities_sum_to_one_for_all_alphas(self, intervals):
        """layer_probabilities must always sum to 1.0 regardless of skew_alpha."""
        for alpha in [-3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0]:
            probs = layer_probabilities(
                z_front_m=0.5,
                sigma_m=0.15,
                intervals_df=intervals,
                total_height_m=1.0,
                skew_alpha=alpha,
            )
            assert float(probs.sum()) == pytest.approx(1.0, abs=1e-8), f"Failed at alpha={alpha}"
            assert (probs >= 0).all(), f"Negative probabilities at alpha={alpha}"

    def test_skew_alpha_zero_matches_existing_layer_probabilities(self, intervals):
        """layer_probabilities with skew_alpha=0 must match the original (no kwarg) call."""
        p_default = layer_probabilities(0.5, 0.15, intervals, 1.0)
        p_explicit = layer_probabilities(0.5, 0.15, intervals, 1.0, skew_alpha=0.0)
        np.testing.assert_array_almost_equal(
            p_default.values, p_explicit.values, decimal=10
        )

    def test_negative_skew_alpha_biases_toward_lower_layers(self, intervals):
        """Negative skew_alpha should shift probability mass toward layers below z_front."""
        z_front = 0.5  # mid-point of the silo
        p_sym = layer_probabilities(z_front, 0.20, intervals, 1.0, skew_alpha=0.0)
        p_skew = layer_probabilities(z_front, 0.20, intervals, 1.0, skew_alpha=-2.0)
        # Layers below front (indices 0, 1: z1 <= 0.5) should have higher relative share.
        lower_sym = p_sym.iloc[:2].sum()
        lower_skew = p_skew.iloc[:2].sum()
        upper_sym = p_sym.iloc[2:].sum()
        upper_skew = p_skew.iloc[2:].sum()
        assert lower_skew > lower_sym
        assert upper_skew < upper_sym

    def test_skew_alpha_changes_simulation_distribution(self, base_kwargs):
        """Nonzero skew_alpha must produce a different distribution than alpha=0."""
        result_sym = _simulate_for_sigma(**base_kwargs, skew_alpha=0.0)
        result_skew = _simulate_for_sigma(**base_kwargs, skew_alpha=-2.0)
        assert not np.allclose(
            result_sym["discharged_mass_kg"].values,
            result_skew["discharged_mass_kg"].values,
        )

    def test_mass_conservation_with_skew_alpha(self, base_kwargs):
        """Total discharged mass must be conserved with skew-normal kernel active."""
        result = _simulate_for_sigma(**base_kwargs, skew_alpha=-2.0)
        assert float(result["discharged_mass_kg"].sum()) == pytest.approx(
            base_kwargs["discharge_mass_kg"], rel=1e-6
        )


# ---------------------------------------------------------------------------
# Integration: all three features together
# ---------------------------------------------------------------------------

class TestAllFeaturesIntegration:
    def test_all_features_active_smoke(self, base_kwargs, layer_moisture_arr):
        """All three physics features active simultaneously must complete without error."""
        result = _simulate_for_sigma(
            **base_kwargs,
            moisture_beta=0.05,
            sigma_alpha=0.4,
            skew_alpha=-2.0,
            layer_moisture=layer_moisture_arr,
        )
        assert float(result["discharged_mass_kg"].sum()) == pytest.approx(
            base_kwargs["discharge_mass_kg"], rel=1e-6
        )
        assert (result["discharged_mass_kg"] >= 0).all()

    def test_all_features_end_to_end_via_run_blend(self):
        """Full pipeline with all three features via RunConfig must produce valid output."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            write_sample_data(base)
            inputs = load_inputs(base)

            result = run_blend(
                inputs,
                RunConfig(
                    moisture_beta=0.05,
                    sigma_alpha=0.4,
                    skew_alpha=-2.0,
                ),
            )
            assert result["total_discharged_mass_kg"] > 0
            assert result["total_remaining_mass_kg"] >= 0
            assert not pd.isna(result["df_lot_contrib_all"]["discharged_mass_kg"]).any()
            assert (result["df_lot_contrib_all"]["discharged_mass_kg"] >= 0).all()

    def test_all_features_off_matches_original_behaviour(self):
        """With all three features set to 0.0, result must match vanilla RunConfig."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            write_sample_data(base)
            inputs = load_inputs(base)

            result_vanilla = run_blend(inputs, RunConfig())
            result_explicit_zeros = run_blend(
                inputs,
                RunConfig(moisture_beta=0.0, sigma_alpha=0.0, skew_alpha=0.0),
            )
            np.testing.assert_array_almost_equal(
                result_vanilla["df_lot_contrib_all"]["discharged_mass_kg"].values,
                result_explicit_zeros["df_lot_contrib_all"]["discharged_mass_kg"].values,
                decimal=10,
            )
