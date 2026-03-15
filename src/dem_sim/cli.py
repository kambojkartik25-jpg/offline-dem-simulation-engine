from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .sample_data import write_sample_data
from .synthetic import generate_synthetic_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dem-sim",
        description="Offline DEM-inspired silo discharge and blend simulator.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init-sample", help="Create sample CSV input files.")
    init_cmd.add_argument("--out", default="data/sample", help="Directory for sample inputs.")

    synth_cmd = sub.add_parser(
        "init-synthetic", help="Create synthetic CSV input files."
    )
    synth_cmd.add_argument("--out", default="data/synthetic", help="Output directory.")
    synth_cmd.add_argument("--seed", type=int, default=42)
    synth_cmd.add_argument("--silos", type=int, default=3)
    synth_cmd.add_argument("--suppliers", type=int, default=3)
    synth_cmd.add_argument("--lots", type=int, default=12)

    validate_cmd = sub.add_parser("validate", help="Validate input CSV files.")
    validate_cmd.add_argument("--in", dest="input_dir", default="data/sample", help="Input directory.")

    run_cmd = sub.add_parser("run", help="Run simulation from input CSV files.")
    run_cmd.add_argument("--in", dest="input_dir", default="data/sample", help="Input directory.")
    run_cmd.add_argument("--out", dest="output_dir", default="outputs/latest", help="Output directory.")
    run_cmd.add_argument("--rho-bulk-kg-m3", type=float, default=610.0)
    run_cmd.add_argument("--grain-diameter-m", type=float, default=0.004)
    run_cmd.add_argument("--beverloo-c", type=float, default=0.58)
    run_cmd.add_argument("--beverloo-k", type=float, default=1.4)
    run_cmd.add_argument("--gravity-m-s2", type=float, default=9.81)
    run_cmd.add_argument("--sigma-m", type=float, default=0.12)
    run_cmd.add_argument("--steps", type=int, default=2000)
    run_cmd.add_argument("--auto-adjust", action="store_true", default=False)
    run_cmd.add_argument(
        "--moisture-beta", type=float, default=0.0,
        help="Cohesion correction exponent (0=disabled). Suggested brewery value: 0.05. "
             "Applies exp(-beta * moisture_pct) to the effective flow rate per timestep.",
    )
    run_cmd.add_argument(
        "--sigma-alpha", type=float, default=0.0,
        help="Sigma height-scaling exponent (0=disabled). Suggested brewery value: 0.4. "
             "Mixing width narrows as silo empties: sigma(t) = sigma_0 * (h_rem/h_init)^alpha.",
    )
    run_cmd.add_argument(
        "--skew-alpha", type=float, default=0.0,
        help="Asymmetric mixing kernel shape parameter (0=symmetric Normal). "
             "Suggested brewery value: -2.0. Negative values bias discharge mass toward "
             "layers below the front, modelling hopper convergence zone mixing.",
    )

    return parser


def _cmd_init_sample(args: argparse.Namespace) -> int:
    write_sample_data(args.out)
    print(f"Sample input files created at {Path(args.out).resolve()}")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from .io import load_inputs
    from .reporting import validate_inputs_shape

    inputs = load_inputs(args.input_dir)
    errors = validate_inputs_shape(inputs)
    if errors:
        print("Validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2
    print("Validation passed.")
    return 0


def _cmd_init_synthetic(args: argparse.Namespace) -> int:
    out = generate_synthetic_dataset(
        output_dir=args.out,
        seed=args.seed,
        n_silos=args.silos,
        n_suppliers=args.suppliers,
        n_lots=args.lots,
    )
    print(f"Synthetic input files created at {out.resolve()}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    from .io import ensure_output_dir, load_inputs
    from .reporting import terminal_summary, validate_inputs_shape, write_outputs
    from .service import RunConfig, run_blend

    inputs = load_inputs(args.input_dir)
    errors = validate_inputs_shape(inputs)
    if errors:
        print("Validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2

    cfg = RunConfig(
        rho_bulk_kg_m3=args.rho_bulk_kg_m3,
        grain_diameter_m=args.grain_diameter_m,
        beverloo_c=args.beverloo_c,
        beverloo_k=args.beverloo_k,
        gravity_m_s2=args.gravity_m_s2,
        sigma_m=args.sigma_m,
        steps=args.steps,
        auto_adjust=args.auto_adjust,
        moisture_beta=args.moisture_beta,
        sigma_alpha=args.sigma_alpha,
        skew_alpha=args.skew_alpha,
    )
    result = run_blend(inputs, cfg)
    out_dir = ensure_output_dir(args.output_dir)
    paths = write_outputs(result, out_dir)

    print(terminal_summary(result))
    print("Artifacts:")
    for label, path in paths.items():
        print(f"- {label}: {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-sample":
        return _cmd_init_sample(args)
    if args.command == "validate":
        return _cmd_validate(args)
    if args.command == "init-synthetic":
        return _cmd_init_synthetic(args)
    if args.command == "run":
        return _cmd_run(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
