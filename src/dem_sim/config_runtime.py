from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, val in extra.items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


DEFAULT_SETTINGS: dict[str, Any] = {
    "profile": "local",
    # Keep bootstrap defaults minimal; canonical runtime defaults live in config/base.json.
    "api": {},
    "brewmaster": {},
}


def load_runtime_settings() -> dict[str, Any]:
    settings = dict(DEFAULT_SETTINGS)
    profile = os.getenv("DEM_SIM_PROFILE", str(DEFAULT_SETTINGS.get("profile", "local"))).strip() or "local"
    cfg_dir = _repo_root() / "config"
    settings = _deep_merge(settings, _read_json_if_exists(cfg_dir / "base.json"))
    settings = _deep_merge(settings, _read_json_if_exists(cfg_dir / f"{profile}.json"))
    settings["profile"] = profile

    env_endpoint = os.getenv("BREWMASTER_ENDPOINT_URL")
    if env_endpoint is not None and env_endpoint.strip():
        settings.setdefault("brewmaster", {})
        settings["brewmaster"]["endpoint_url"] = env_endpoint.strip()
    env_verify = os.getenv("BREWMASTER_VERIFY_TLS")
    if env_verify is not None:
        settings.setdefault("brewmaster", {})
        settings["brewmaster"]["verify_tls"] = env_verify.strip().lower() in {"1", "true", "yes", "on"}
    return settings
