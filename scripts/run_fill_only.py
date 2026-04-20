from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dem_sim.state import reset_state, set_state, run_fill_only_simulation
from dem_sim.web import _sample_payload


def main() -> int:
    payload = _sample_payload()
    reset_state()
    set_state(
        silos=payload["silos"],
        layers=payload["layers"],
        suppliers=payload.get("suppliers", []),
        incoming_queue=payload.get("incoming_queue", []),
        action="bootstrap_sample_state",
        meta={"source": "sample_payload"},
    )

    out = run_fill_only_simulation()
    summary = out.get("summary", {})

    print("run_fill_only_simulation summary")
    print(f"  silos: {len(summary.get('silos', []))}")
    print(f"  incoming_queue_count: {summary.get('incoming_queue', {}).get('count')}")
    print(f"  incoming_queue_mass_kg: {summary.get('incoming_queue', {}).get('total_mass_kg')}")
    print(f"  cumulative_discharged_kg: {summary.get('cumulative_discharged_kg')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
