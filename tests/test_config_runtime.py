from __future__ import annotations

from dem_sim.config_runtime import load_runtime_settings
from dem_sim.web import ProductionPlanApplyRequest


def test_runtime_settings_load_from_base_json() -> None:
    settings = load_runtime_settings()
    assert settings["api"]["fixed_discharge_target_kg"] == 9000.0
    assert settings["api"]["fixed_discharge_tol_kg"] == 0.001
    assert settings["api"]["default_steps"] == 800
    assert "endpoint_url" in settings["brewmaster"]
    assert "timeout_s" in settings["brewmaster"]
    assert "verify_tls" in settings["brewmaster"]


def test_runtime_settings_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("BREWMASTER_ENDPOINT_URL", "https://example.test/score")
    monkeypatch.setenv("BREWMASTER_VERIFY_TLS", "true")
    settings = load_runtime_settings()
    assert settings["brewmaster"]["endpoint_url"] == "https://example.test/score"
    assert settings["brewmaster"]["verify_tls"] is True


def test_production_plan_apply_request_requires_reason() -> None:
    req = ProductionPlanApplyRequest(
        candidate_index=0,
        reason="operator selected best fit",
        config={"steps": 200},
    )
    assert req.reason == "operator selected best fit"

