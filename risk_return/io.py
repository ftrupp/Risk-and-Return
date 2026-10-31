"""Explicit local CSV/JSON I/O. No pickle loading, downloads or implicit writes."""
import csv
import hashlib
import importlib.metadata
import json
from pathlib import Path

import pandas as pd

from . import __version__
from .core import parse_date, validate_metadata, validate_prices


def load_dataset(prices_path, metadata_path):
    prices_path, metadata_path = Path(prices_path), Path(metadata_path)
    with prices_path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream))
    if not rows or rows[0][0:1] != ["date"] or len(rows[0]) < 2 or len(set(rows[0])) != len(rows[0]):
        raise ValueError("CSV must start with date and unique asset columns")
    if any(len(row) != len(rows[0]) for row in rows[1:]):
        raise ValueError("CSV rows must have the same number of fields as the header")
    prices = pd.DataFrame([row[1:] for row in rows[1:]], columns=rows[0][1:],
                          index=pd.DatetimeIndex([parse_date(row[0]) for row in rows[1:]], name="date"))
    prices = prices.replace("", float("nan")).astype(float)
    prices = validate_prices(prices)
    metadata = validate_metadata(json.loads(metadata_path.read_text(encoding="utf-8")), prices)
    fingerprints = {"prices_sha256": hashlib.sha256(prices_path.read_bytes()).hexdigest(),
                    "metadata_sha256": hashlib.sha256(metadata_path.read_bytes()).hexdigest()}
    return prices, metadata, fingerprints


def export_run(result, output, fingerprints=None):
    """Create a new directory; never overwrite a prior experiment."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    for name in ("metrics", "weights", "costs"):
        result[name].to_csv(output / f"{name}.csv", index=False, float_format="%.12g")
    result["trajectory"].to_csv(output / "trajectory.csv", index_label="date", float_format="%.12g")
    # DataFrame.to_json maps missing/undefined statistics to JSON null, not NaN.
    metrics = json.loads(result["metrics"].to_json(orient="records", double_precision=15))
    summary = {"package_version": __version__, "config": result["config"], "dataset": result["dataset"],
               "input_fingerprints": fingerprints or {}, "metrics": metrics,
               "versions": {name: importlib.metadata.version(name) for name in ("numpy", "pandas", "scipy")},
               "convention": "buy_and_hold_next_close_entry_full_exit; first wealth before entry; entry cost in first return; exit cost in last return",
               "risk_free_status": "constant_effective_annual_assumption_not_historical_observation",
               "operability_claim": False}
    (output / "run.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output / "quality.json").write_text(json.dumps(result["quality"], indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return summary
