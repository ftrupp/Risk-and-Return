"""Command-line entry point; all data access is explicit and local."""
import argparse
import hashlib
import json
import sys

from . import __version__
from .core import Config, METHODS, run_experiment, synthetic_dataset
from .io import export_run, load_dataset


def main(argv=None):
    parser = argparse.ArgumentParser(description="Offline, reproducible portfolio research (not trading).")
    parser.add_argument("--version", action="version", version=__version__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--synthetic", action="store_true", help="Use deterministic invented prices")
    source.add_argument("--prices", help="Local CSV with date followed by adjusted-close asset columns")
    parser.add_argument("--metadata", help="Required JSON declarations for --prices")
    parser.add_argument("--output", required=True, help="New output directory; existing directories are rejected")
    parser.add_argument("--methods", nargs="+", choices=METHODS, default=list(METHODS))
    parser.add_argument("--train-periods", type=int, default=252)
    parser.add_argument("--test-periods", type=int, default=126)
    parser.add_argument("--periods-per-year", type=int, default=252)
    parser.add_argument("--initial-capital", type=float, default=10000.)
    parser.add_argument("--max-weight", type=float, default=.60)
    parser.add_argument("--cost-bps", type=float, default=10., help="Assumed one-way proportional entry/exit costs")
    parser.add_argument("--risk-free-annual", type=float, default=0., help="Constant effective annual assumption, decimal")
    parser.add_argument("--test-end", help="Fixed final session YYYY-MM-DD (default: last available)")
    args = parser.parse_args(argv)
    if bool(args.prices) != bool(args.metadata):
        parser.error("--prices and --metadata must be supplied together")
    try:
        config = Config(**{field: getattr(args, field) for field in Config.__dataclass_fields__}).validate()
        if args.synthetic:
            prices, metadata = synthetic_dataset(config.train_periods + config.test_periods + 2)
            fingerprints = {"generated_prices_sha256": hashlib.sha256(prices.to_csv(float_format="%.12g").encode()).hexdigest()}
        else:
            prices, metadata, fingerprints = load_dataset(args.prices, args.metadata)
        result = run_experiment(prices, metadata, config, tuple(args.methods))
        summary = export_run(result, args.output, fingerprints)
        print(json.dumps({"dataset_kind": metadata["kind"], "output": args.output, "metrics": summary["metrics"]}, indent=2))
        return 0 if result["metrics"]["status"].eq("ok").all() else 2
    except (ValueError, OSError, RuntimeError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
