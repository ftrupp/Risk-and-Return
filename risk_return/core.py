"""Pure portfolio calculations shared by the CLI and both educational notebooks."""
from dataclasses import asdict, dataclass
import re

import numpy as np
import pandas as pd

METHODS = ("equal_weight", "min_volatility", "max_sharpe")


@dataclass(frozen=True)
class Config:
    train_periods: int = 252
    test_periods: int = 126
    periods_per_year: int = 252
    initial_capital: float = 10000.0
    max_weight: float = 0.60
    cost_bps: float = 10.0
    risk_free_annual: float = 0.0
    test_end: str | None = None

    def validate(self):
        for field in ("train_periods", "test_periods", "periods_per_year"):
            value = getattr(self, field)
            if type(value) is not int or value < (1 if field == "periods_per_year" else 2):
                raise ValueError(f"{field} must be an integer with enough observations")
        numbers = [self.initial_capital, self.max_weight, self.cost_bps, self.risk_free_annual]
        if not np.isfinite(numbers).all():
            raise ValueError("Configuration values must be finite")
        if self.initial_capital <= 0 or not 0 < self.max_weight <= 1:
            raise ValueError("Capital must be positive and max_weight in (0, 1]")
        if not 0 <= self.cost_bps < 10000 or self.risk_free_annual <= -1:
            raise ValueError("Costs must be in [0, 10000) bps and risk-free rate above -1")
        if self.test_end is not None:
            parse_date(self.test_end)
        return self


def parse_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Dates must use YYYY-MM-DD")
    return pd.Timestamp(value)


def validate_prices(prices, *, complete=False):
    if not isinstance(prices, pd.DataFrame) or len(prices) < 3 or prices.shape[1] == 0:
        raise ValueError("At least three dated prices and one asset are required")
    if not isinstance(prices.index, pd.DatetimeIndex) or prices.index.hasnans:
        raise ValueError("A valid DatetimeIndex is required")
    if prices.index.tz is not None or not prices.index.equals(prices.index.normalize()):
        raise ValueError("Use timezone-free session dates, not intraday timestamps")
    if not prices.index.is_unique or not prices.index.is_monotonic_increasing:
        raise ValueError("Dates must be unique and sorted; no silent sorting")
    if not prices.columns.is_unique or any(
        not isinstance(x, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}", x)
        for x in prices.columns
    ):
        raise ValueError("Asset identifiers must be unique, simple alphanumeric labels")
    values = prices.to_numpy(dtype=float)
    if np.isinf(values).any() or ((values <= 0) & ~np.isnan(values)).any():
        raise ValueError("Observed prices must be finite and positive")
    if complete and not np.isfinite(values).all():
        raise ValueError("Missing held-asset prices; no filling, dropping dates or substitutions")
    return prices.astype(float)


def validate_metadata(metadata, prices):
    """Validate declarations; this cannot independently certify provider accuracy."""
    required = {"schema_version", "dataset_id", "kind", "currency", "unit", "price_basis",
                "frequency", "periods_per_year", "coverage_start", "coverage_end", "source",
                "publication_date", "retrieved_at", "license", "point_in_time_universe",
                "dividends_included", "limitations"}
    if not isinstance(metadata, dict) or required - metadata.keys():
        raise ValueError(f"Metadata requires fields: {', '.join(sorted(required))}")
    if type(metadata["schema_version"]) is not int or metadata["schema_version"] != 1:
        raise ValueError("Unsupported metadata schema_version")
    if metadata["kind"] not in ("synthetic", "observed"):
        raise ValueError("kind must be synthetic or observed")
    if not isinstance(metadata["currency"], str) or not re.fullmatch("[A-Z]{3}", metadata["currency"]):
        raise ValueError("Declare one three-letter currency; currency conversion is not implemented")
    if metadata["unit"] != "currency_per_adjusted_unit" or metadata["price_basis"] != "total_return_adjusted_close":
        raise ValueError("Expected total-return adjusted closes in currency_per_adjusted_unit")
    if metadata["frequency"] != "session_daily":
        raise ValueError("Only session_daily prices are supported")
    if type(metadata["periods_per_year"]) is not int or metadata["periods_per_year"] <= 0:
        raise ValueError("periods_per_year must be a positive integer")
    for field in ("dataset_id", "source", "license"):
        if not isinstance(metadata[field], str) or not metadata[field].strip():
            raise ValueError(f"{field} must be a nonempty declaration")
    for field in ("point_in_time_universe", "dividends_included"):
        if metadata[field] is not None and type(metadata[field]) is not bool:
            raise ValueError(f"{field} must be true, false or null")
    if metadata["dividends_included"] is not True:
        raise ValueError("Total-return adjusted closes require dividends_included=true")
    if not isinstance(metadata["limitations"], list) or any(not isinstance(x, str) for x in metadata["limitations"]):
        raise ValueError("limitations must be a list of strings")
    for field in ("publication_date", "retrieved_at"):
        if metadata[field] is not None:
            parse_date(metadata[field])
    if metadata["kind"] == "observed" and metadata["retrieved_at"] is None:
        raise ValueError("Observed data must declare retrieval date")
    if parse_date(metadata["coverage_start"]) != prices.index[0] or parse_date(metadata["coverage_end"]) != prices.index[-1]:
        raise ValueError("Declared coverage must match the actual CSV dates")
    return dict(metadata)


def synthetic_dataset(periods=380):
    """Deterministic, invented paths; no fitted parameters or market observations."""
    if type(periods) is not int or periods < 6:
        raise ValueError("Synthetic data requires at least six prices")
    t = np.arange(periods - 1, dtype=float)
    returns = np.column_stack([
        .0005 + .008 * np.sin(.17 * t) + .004 * np.cos(.037 * t),
        .00025 + .004 * np.sin(.11 * t + 1) + .003 * np.cos(.29 * t),
        .00035 + .007 * np.sin(.07 * t + 2) + .005 * np.cos(.19 * t),
    ])
    levels = np.vstack([np.ones(3), np.cumprod(1 + returns, axis=0)]) * [100., 50., 200.]
    prices = pd.DataFrame(levels, index=pd.bdate_range("2020-01-02", periods=periods),
                          columns=["Asset_A", "Asset_B", "Asset_C"])
    prices.index.name = "date"
    metadata = {"schema_version": 1, "dataset_id": "synthetic-trigonometric-v1",
                "kind": "synthetic", "currency": "USD", "unit": "currency_per_adjusted_unit",
                "price_basis": "total_return_adjusted_close", "frequency": "session_daily",
                "periods_per_year": 252, "coverage_start": str(prices.index[0].date()),
                "coverage_end": str(prices.index[-1].date()), "source": "risk_return.core.synthetic_dataset",
                "publication_date": None, "retrieved_at": None, "license": "CC0-1.0",
                "point_in_time_universe": True, "dividends_included": True,
                "limitations": ["Invented prices in simulated USD; no empirical calibration or market returns.",
                                "Weekday calendar without exchange holidays; no separate dividends."]}
    return prices, metadata


def annualized_rf(rate, periods_per_year):
    if not np.isfinite([rate, periods_per_year]).all() or rate <= -1 or periods_per_year <= 0:
        raise ValueError("Invalid risk-free rate or frequency")
    return float(periods_per_year * np.expm1(np.log1p(rate) / periods_per_year))


def checked_weights(weights, labels, cap=1.0):
    if not isinstance(weights, pd.Series) or not weights.index.is_unique or set(weights.index) != set(labels):
        raise ValueError("Weights must be a labeled Series matching the asset set")
    w = weights.reindex(labels).astype(float)
    if not np.isfinite(w).all() or w.min() < -1e-8 or w.max() > cap + 1e-8 or abs(w.sum() - 1) > 1e-8:
        raise ValueError("Invalid weights: long-only, fully invested and capped constraints required")
    w = w.clip(lower=0) / w.clip(lower=0).sum()
    if w.max() > cap + 1e-8:
        raise ValueError("Weight normalization violated the cap")
    return w


def optimize_weights(mu, covariance, method="min_volatility", cap=1., rfr=0.):
    """Small constrained SLSQP problem; inputs are training moments only."""
    from scipy.optimize import minimize
    if method not in ("min_volatility", "max_sharpe"):
        raise ValueError("Unknown optimization method")
    if not isinstance(mu, pd.Series) or not isinstance(covariance, pd.DataFrame):
        raise ValueError("Labeled moments are required")
    if not covariance.index.equals(mu.index) or not covariance.columns.equals(mu.index) or not mu.index.is_unique:
        raise ValueError("Return and covariance labels/order must agree")
    matrix, means = covariance.to_numpy(dtype=float), mu.to_numpy(dtype=float)
    n = len(mu)
    if n == 0 or not np.isfinite([cap, rfr]).all() or not 0 < cap <= 1 or n * cap < 1 - 1e-12:
        raise ValueError("Infeasible cap or invalid optimization parameters")
    if not np.isfinite(means).all() or not np.isfinite(matrix).all() or not np.allclose(matrix, matrix.T, atol=1e-12):
        raise ValueError("Moments must be finite and covariance symmetric")
    eigenvalues = np.linalg.eigvalsh(matrix)
    if eigenvalues.min() < -1e-10:
        raise ValueError("Covariance must be positive semidefinite")
    equal = np.full(n, 1 / n)
    starts = [equal]
    if method == "max_sharpe":
        if eigenvalues.min() <= 1e-12:
            raise ValueError("Maximum Sharpe requires nondegenerate positive risk")
        # The largest *feasible* excess matters when an individual weight is capped.
        best_return = np.zeros(n)
        budget = 1.
        for i in np.argsort(-means, kind="stable"):
            best_return[i] = min(cap, budget)
            budget -= best_return[i]
        if means @ best_return <= rfr:
            raise ValueError("No feasible portfolio has positive expected excess return")
        starts.append(best_return)
        def objective(w):
            variance = float(w @ matrix @ w)
            return -(means @ w - rfr) / np.sqrt(variance) if variance > 1e-24 else np.inf
    else:
        scale = max(float(np.diag(matrix).max()), 1e-12)
        def objective(w):
            return float(w @ matrix @ w) / scale
    if n == 1 or np.isclose(n * cap, 1, atol=1e-12, rtol=0):
        return checked_weights(pd.Series(equal, index=mu.index), mu.index, cap)
    valid = []
    for start in starts:
        result = minimize(objective, start, method="SLSQP", bounds=[(0., cap)] * n,
                          constraints={"type": "eq", "fun": lambda w: w.sum() - 1},
                          options={"ftol": 1e-12, "maxiter": 1000})
        if result.success and np.isfinite(result.fun):
            try:
                w = checked_weights(pd.Series(result.x, index=mu.index), mu.index, cap)
                if np.isfinite(objective(w.to_numpy())):
                    valid.append(w)
            except ValueError:
                pass
    if not valid:
        raise RuntimeError("Optimizer did not return successful feasible weights")
    return min(valid, key=lambda w: objective(w.to_numpy()))


def simulate_buy_and_hold(prices, weights, initial_capital=10000., cost_bps=0.):
    """Fixed adjusted units, entry at first close and full liquidation at last close.

    Row 0 is capital immediately BEFORE entry. Entry cost appears in the first
    observed return, exit cost in the last. All metrics use this same net path.
    Entry notional is capital/(1+c), so notional plus fee equals available cash.
    """
    prices = validate_prices(prices)
    w = checked_weights(weights, prices.columns)
    if not np.isfinite([initial_capital, cost_bps]).all() or initial_capital <= 0 or not 0 <= cost_bps < 10000:
        raise ValueError("Invalid capital or costs")
    active = w[w > 0]
    held = validate_prices(prices[active.index], complete=True)
    c = cost_bps / 10000
    invested = initial_capital / (1 + c)
    units = active * invested / held.iloc[0]
    marked = held.mul(units).sum(axis=1, skipna=False)
    wealth = marked.copy()
    wealth.iloc[0] = initial_capital
    wealth.iloc[-1] *= 1 - c
    wealth.name = "wealth"
    costs = {"entry_notional": invested, "entry_cost": invested * c,
             "exit_notional": float(marked.iloc[-1]), "exit_cost": float(marked.iloc[-1] * c)}
    return wealth, units, costs


def wealth_metrics(wealth, periods_per_year=252, risk_free_annual=0.):
    validate_prices(wealth.rename("wealth").to_frame(), complete=True)
    rfr = annualized_rf(risk_free_annual, periods_per_year)
    returns = wealth.pct_change(fill_method=None).iloc[1:]
    years = (wealth.index[-1] - wealth.index[0]).days / 365.25
    total = float(wealth.iloc[-1] / wealth.iloc[0] - 1)
    mean = float(returns.mean() * periods_per_year)
    volatility = float(returns.std(ddof=1) * np.sqrt(periods_per_year))
    return {"start": str(wealth.index[0].date()), "end": str(wealth.index[-1].date()),
            "periods": len(returns), "calendar_years": years, "total_return": total,
            "cagr": float(np.expm1(np.log1p(total) / years)), "annual_mean_return": mean,
            "annual_volatility": volatility, "sharpe": (mean - rfr) / volatility if volatility > 1e-12 else None,
            "max_drawdown": float((wealth / wealth.cummax() - 1).min()),
            "terminal_wealth": float(wealth.iloc[-1])}


def run_experiment(prices, metadata, config=None, methods=METHODS):
    config = (config or Config()).validate()
    prices = validate_prices(prices)
    metadata = validate_metadata(metadata, prices)
    if config.periods_per_year != metadata["periods_per_year"]:
        raise ValueError("Configured annualization disagrees with dataset metadata")
    if not methods or len(set(methods)) != len(methods) or set(methods) - set(METHODS):
        raise ValueError("Select unique supported methods")
    if config.test_end is not None:
        date = parse_date(config.test_end)
        if date not in prices.index:
            raise ValueError("test_end must be an observed session date")
        window = prices.loc[:date]
    else:
        window = prices
    needed = config.train_periods + config.test_periods + 2
    if len(window) < needed:
        raise ValueError(f"Insufficient history: need {needed} prices")
    window = window.iloc[-needed:]
    train = window.iloc[:config.train_periods + 1]
    test = window.iloc[config.train_periods + 1:]
    eligible = train.columns[train.notna().all()]
    if len(eligible) == 0 or len(eligible) * config.max_weight < 1 - 1e-12:
        raise ValueError("Training coverage leaves no feasible capped portfolio")
    train, test = train[eligible], test[eligible]
    returns = train.pct_change(fill_method=None).iloc[1:]
    mu = returns.mean() * config.periods_per_year
    covariance = returns.cov() * config.periods_per_year
    rfr = annualized_rf(config.risk_free_annual, config.periods_per_year)
    quality = {"input_assets": len(prices.columns), "eligible_training": list(eligible),
               "excluded_training": list(prices.columns.difference(eligible)),
               "missing_input_prices": int(prices.isna().sum().sum()),
               "missing_test_prices": {x: int(test[x].isna().sum()) for x in test if test[x].isna().any()},
               "train_start": str(train.index[0].date()), "train_end": str(train.index[-1].date()),
               "test_start": str(test.index[0].date()), "test_end": str(test.index[-1].date()),
               "calendar_verified": False, "metadata_independently_verified": False}
    metrics, weight_rows, cost_rows = [], [], []
    trajectory = pd.DataFrame(index=test.index)
    for method in methods:
        row = {"strategy": method, "status": "incomplete", "reason": ""}
        try:
            weights = (pd.Series(1 / len(eligible), index=eligible) if method == "equal_weight" else
                       optimize_weights(mu, covariance, method, config.max_weight, rfr))
            weights = checked_weights(weights, eligible, config.max_weight)
            # Save the decision even if subsequent held prices make valuation impossible.
            weight_rows.extend({"strategy": method, "asset": asset, "weight": float(weight)}
                               for asset, weight in weights.items())
            wealth, units, costs = simulate_buy_and_hold(test, weights, config.initial_capital, config.cost_bps)
            trajectory[method] = wealth
            row.update(wealth_metrics(wealth, config.periods_per_year, config.risk_free_annual))
            model_vol = float(np.sqrt(max(0., weights @ covariance @ weights)))
            row.update(status="ok", train_model_mean=float(mu @ weights), train_model_volatility=model_vol)
            cost_rows.append({"strategy": method, **costs})
        except (ValueError, RuntimeError) as error:
            row["reason"] = str(error)
        metrics.append(row)
    return {"config": asdict(config), "dataset": metadata, "quality": quality,
            "metrics": pd.DataFrame(metrics), "weights": pd.DataFrame(weight_rows, columns=["strategy", "asset", "weight"]),
            "trajectory": trajectory, "costs": pd.DataFrame(cost_rows, columns=["strategy", "entry_notional", "entry_cost", "exit_notional", "exit_cost"])}
