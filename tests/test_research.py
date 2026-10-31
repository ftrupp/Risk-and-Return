from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
import json
import os
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from risk_return import core
from risk_return.cli import main
from risk_return.io import export_run, load_dataset


def moments(covariance=None):
    names = ["A", "B"]
    covariance = np.diag([.04, .09]) if covariance is None else covariance
    return pd.Series([.10, .11], index=names), pd.DataFrame(covariance, index=names, columns=names)


class MathematicalTests(unittest.TestCase):
    def test_analytic_minimum_variance(self):
        mu, cov = moments()
        w = core.optimize_weights(mu, cov)
        np.testing.assert_allclose(w, [9/13, 4/13], atol=1e-6)
        self.assertAlmostEqual(float(np.sqrt(w @ cov @ w)), np.sqrt(9/325), places=9)

    def test_binding_cap(self):
        mu, cov = moments()
        np.testing.assert_allclose(core.optimize_weights(mu, cov, cap=.6), [.6, .4], atol=1e-6)

    def test_exact_equal_weight_cap(self):
        mu, cov = moments()
        np.testing.assert_allclose(core.optimize_weights(mu, cov, cap=.5), [.5, .5])

    def test_infeasible_cap_rejected(self):
        with self.assertRaisesRegex(ValueError, "Infeasible"):
            core.optimize_weights(*moments(), cap=.4)

    def test_negative_covariance_is_valid(self):
        mu, cov = moments([[.01, -.02], [-.02, .09]])
        np.testing.assert_allclose(core.optimize_weights(mu, cov), [11/14, 3/14], atol=1e-6)

    def test_invalid_covariance_rejected(self):
        for matrix in ([[1, 2], [2, 1]], [[1, .2], [.1, 1]]):
            with self.subTest(matrix=matrix), self.assertRaises(ValueError):
                core.optimize_weights(*moments(matrix))

    def test_analytic_maximum_sharpe(self):
        np.testing.assert_allclose(core.optimize_weights(*moments(), method="max_sharpe", rfr=.02),
                                   [2/3, 1/3], atol=2e-6)

    def test_maximum_sharpe_requires_feasible_positive_excess(self):
        mu, cov = moments()
        mu[:] = [.1, -.3]
        with self.assertRaisesRegex(ValueError, "feasible portfolio"):
            core.optimize_weights(mu, cov, method="max_sharpe", cap=.5)

    def test_maximum_sharpe_handles_mixed_excess_without_reciprocal(self):
        mu, cov = moments()
        mu[:] = [-.2, .1]
        np.testing.assert_allclose(core.optimize_weights(mu, cov, method="max_sharpe"), [0., 1.], atol=1e-6)

    def test_maximum_sharpe_rejects_zero_risk(self):
        with self.assertRaisesRegex(ValueError, "risk"):
            core.optimize_weights(*moments(np.zeros((2, 2))), method="max_sharpe")

    def test_solver_failure_and_invalid_claimed_success(self):
        for success, w in [(False, [.5, .5]), (True, [.5, .8]), (True, [np.nan, np.nan])]:
            result = SimpleNamespace(success=success, x=np.array(w), fun=.1)
            with self.subTest(success=success, weights=w), patch("scipy.optimize.minimize", return_value=result):
                with self.assertRaises(RuntimeError):
                    core.optimize_weights(*moments())

    def test_label_mismatch_rejected(self):
        mu, cov = moments()
        with self.assertRaises(ValueError):
            core.optimize_weights(mu.iloc[::-1], cov)


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.prices = pd.DataFrame({"A": [100., 200., 100.], "B": [10., 10., 20.]},
                                   index=pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"]))
        self.weights = pd.Series({"A": .25, "B": .75})

    def test_known_buy_and_hold(self):
        wealth, units, costs = core.simulate_buy_and_hold(self.prices, self.weights, 1000)
        np.testing.assert_allclose(wealth, [1000., 1250., 1750.])
        np.testing.assert_allclose(units, [2.5, 75.])
        self.assertEqual(costs["entry_cost"], 0)
        metrics = core.wealth_metrics(wealth, periods_per_year=1)
        self.assertAlmostEqual(metrics["total_return"], .75)
        self.assertAlmostEqual(metrics["cagr"], 1.75**(365.25/731)-1)
        self.assertAlmostEqual(metrics["annual_mean_return"], .325)
        self.assertAlmostEqual(metrics["annual_volatility"], .15/np.sqrt(2))
        self.assertAlmostEqual(metrics["sharpe"], .325/(.15/np.sqrt(2)))

    def test_entry_exit_cost_budget_and_same_path(self):
        wealth, units, costs = core.simulate_buy_and_hold(self.prices, self.weights, 1000, 100)
        expected = np.array([1000., 1250/1.01, 1750*.99/1.01])
        np.testing.assert_allclose(wealth, expected)
        self.assertAlmostEqual(costs["entry_notional"] + costs["entry_cost"], 1000)
        self.assertAlmostEqual(costs["exit_notional"] - costs["exit_cost"], wealth.iloc[-1])
        metrics = core.wealth_metrics(wealth, 1)
        returns = expected[1:]/expected[:-1]-1
        self.assertAlmostEqual(metrics["total_return"], expected[-1]/expected[0]-1)
        self.assertAlmostEqual(metrics["annual_volatility"], np.std(returns, ddof=1))
        self.assertAlmostEqual(metrics["sharpe"], np.mean(returns)/np.std(returns, ddof=1))

    def test_price_scale_and_weight_label_invariance(self):
        expected = core.simulate_buy_and_hold(self.prices, self.weights)[0]
        scaled = self.prices * [137, .03]
        actual = core.simulate_buy_and_hold(scaled[["B", "A"]], self.weights)[0]
        assert_series_equal(expected, actual)

    def test_missing_held_prices_not_filled(self):
        self.prices.iloc[1, 0] = np.nan
        with self.assertRaisesRegex(ValueError, "Missing held"):
            core.simulate_buy_and_hold(self.prices, self.weights)
        wealth, _, _ = core.simulate_buy_and_hold(self.prices, pd.Series({"A": 0., "B": 1.}))
        np.testing.assert_allclose(wealth, [10000, 10000, 20000])

    def test_zero_volatility_has_undefined_sharpe(self):
        wealth = pd.Series([100., 100., 100.], index=self.prices.index)
        self.assertIsNone(core.wealth_metrics(wealth)["sharpe"])

    def test_periodic_rf_units(self):
        self.assertAlmostEqual(core.annualized_rf(.21, 2), .2)
        wealth = core.simulate_buy_and_hold(self.prices, self.weights)[0]
        self.assertAlmostEqual(core.wealth_metrics(wealth, 2, .21)["sharpe"], 3.)

    def test_invalid_prices_weights_and_costs_rejected(self):
        for cost in [-1, 10000, float("inf")]:
            with self.subTest(cost=cost), self.assertRaises(ValueError):
                core.simulate_buy_and_hold(self.prices, self.weights, cost_bps=cost)
        with self.assertRaises(ValueError):
            core.simulate_buy_and_hold(self.prices, self.weights*2)
        for price in [0, -1, float("inf")]:
            panel = self.prices.copy()
            panel.iloc[1, 0] = price
            with self.subTest(price=price), self.assertRaises(ValueError):
                core.validate_prices(panel)


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        self.prices, self.metadata = core.synthetic_dataset(24)
        self.config = core.Config(train_periods=14, test_periods=8, max_weight=.6)

    def test_synthetic_is_deterministic(self):
        p2, m2 = core.synthetic_dataset(24)
        assert_frame_equal(self.prices, p2)
        self.assertEqual(self.metadata, m2)

    def test_disjoint_windows_and_shared_metric_dates(self):
        result = core.run_experiment(self.prices, self.metadata, self.config, ("equal_weight", "min_volatility"))
        quality = result["quality"]
        self.assertEqual(quality["train_end"], str(self.prices.index[14].date()))
        self.assertEqual(quality["test_start"], str(self.prices.index[15].date()))
        self.assertTrue(result["metrics"]["status"].eq("ok").all())
        for method in ("equal_weight", "min_volatility"):
            recomputed = core.wealth_metrics(result["trajectory"][method])
            row = result["metrics"].set_index("strategy").loc[method]
            for field, value in recomputed.items():
                if isinstance(value, float):
                    self.assertAlmostEqual(row[field], value)
                else:
                    self.assertEqual(row[field], value)

    def test_future_changes_do_not_change_training_decisions(self):
        result = core.run_experiment(self.prices, self.metadata, self.config)
        altered = self.prices.copy()
        altered.iloc[15:, 0] = np.nan
        altered.iloc[15:, 1:] *= 1000
        changed = core.run_experiment(altered, self.metadata, self.config)
        assert_frame_equal(result["weights"], changed["weights"])
        self.assertEqual(result["quality"]["eligible_training"], changed["quality"]["eligible_training"])
        self.assertEqual(changed["metrics"].set_index("strategy").loc["equal_weight", "status"], "incomplete")

    def test_training_missing_assets_excluded_with_audit(self):
        prices = self.prices.copy()
        prices.iloc[3, 0] = np.nan
        result = core.run_experiment(prices, self.metadata, self.config, ("equal_weight",))
        self.assertEqual(result["quality"]["excluded_training"], ["Asset_A"])
        self.assertEqual(result["weights"]["asset"].tolist(), ["Asset_B", "Asset_C"])

    def test_fixed_cutoff_ignores_appended_future(self):
        extended, metadata = core.synthetic_dataset(30)
        config = replace(self.config, test_end=str(self.prices.index[-1].date()))
        before = core.run_experiment(self.prices, self.metadata, config)
        after = core.run_experiment(extended, metadata, config)
        assert_frame_equal(before["weights"], after["weights"])
        assert_frame_equal(before["metrics"], after["metrics"])

    def test_metadata_units_frequency_and_unknowns(self):
        for field, value in [("unit", "percent"), ("currency", "mixed"), ("frequency", "monthly"),
                             ("dividends_included", None), ("point_in_time_universe", "yes"),
                             ("coverage_end", "2020-01-01"), ("kind", "plausible")]:
            metadata = dict(self.metadata, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                core.run_experiment(self.prices, metadata, self.config)

    def test_observed_data_requires_retrieval_date(self):
        with self.assertRaisesRegex(ValueError, "retrieval"):
            core.run_experiment(self.prices, dict(self.metadata, kind="observed"), self.config)

    def test_wrong_dates_and_unmet_history_rejected(self):
        with self.assertRaises(ValueError):
            core.validate_prices(self.prices.iloc[::-1])
        duplicated = self.prices.copy()
        duplicated.index = [self.prices.index[0]] * len(duplicated)
        with self.assertRaises(ValueError):
            core.validate_prices(duplicated)
        with self.assertRaisesRegex(ValueError, "history"):
            core.run_experiment(self.prices, self.metadata, core.Config())
        with self.assertRaisesRegex(ValueError, "annualization"):
            core.run_experiment(self.prices, self.metadata, replace(self.config, periods_per_year=12))

    def test_full_failed_strategy_keeps_weights_and_reason(self):
        prices = self.prices.copy()
        prices.iloc[20, 0] = np.nan
        result = core.run_experiment(prices, self.metadata, self.config, ("equal_weight",))
        self.assertEqual(len(result["weights"]), 3)
        self.assertEqual(result["metrics"].iloc[0]["status"], "incomplete")
        self.assertIn("Missing held", result["metrics"].iloc[0]["reason"])
        self.assertEqual(result["trajectory"].shape[1], 0)

    def test_no_network_in_experiment(self):
        with patch("socket.socket.connect", side_effect=AssertionError("network forbidden")):
            core.run_experiment(self.prices, self.metadata, self.config)


class IOAndCLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get("RISK_RETURN_TEST_TMP"))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.prices, self.metadata = core.synthetic_dataset(24)
        self.csv = self.root/"prices.csv"
        self.meta = self.root/"meta.json"
        self.prices.to_csv(self.csv)
        self.meta.write_text(json.dumps(self.metadata), encoding="utf-8")

    def test_local_roundtrip_and_fingerprints(self):
        prices, metadata, fingerprints = load_dataset(self.csv, self.meta)
        np.testing.assert_allclose(prices, self.prices)
        self.assertEqual(metadata, self.metadata)
        self.assertEqual(len(fingerprints["prices_sha256"]), 64)

    def test_duplicate_headers_extra_fields_and_intraday_rejected(self):
        for contents in ["date,A,A\n2020-01-01,1,2\n", "date,A\n2020-01-01,1,2\n",
                         "date,A\n2020-01-01T12:00:00,1\n"]:
            self.csv.write_text(contents, encoding="utf-8")
            with self.subTest(contents=contents), self.assertRaises(ValueError):
                load_dataset(self.csv, self.meta)

    def test_cli_exports_local_dataset_and_does_not_overwrite(self):
        args = ["--prices", str(self.csv), "--metadata", str(self.meta), "--output", str(self.root/"run"),
                "--train-periods", "14", "--test-periods", "8", "--methods", "equal_weight", "min_volatility"]
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            self.assertEqual(main(args), 0)
            self.assertEqual(main(args), 2)
        summary = json.loads((self.root/"run/run.json").read_text())
        self.assertFalse(summary["operability_claim"])
        self.assertEqual(summary["config"]["cost_bps"], 10)
        self.assertEqual({p.name for p in (self.root/"run").iterdir()},
                         {"metrics.csv", "weights.csv", "costs.csv", "trajectory.csv", "run.json", "quality.json"})

    def test_cli_synthetic_reproducibility(self):
        for folder in ["one", "two"]:
            with redirect_stdout(StringIO()):
                self.assertEqual(main(["--synthetic", "--output", str(self.root/folder)]), 0)
        for filename in ["run.json", "weights.csv", "metrics.csv", "trajectory.csv"]:
            self.assertEqual((self.root/"one"/filename).read_bytes(), (self.root/"two"/filename).read_bytes())

    def test_cli_incomplete_status_is_nonzero_and_reported(self):
        self.prices.iloc[20, 0] = np.nan
        self.prices.to_csv(self.csv)
        with redirect_stdout(StringIO()):
            code = main(["--prices", str(self.csv), "--metadata", str(self.meta), "--output", str(self.root/"bad"),
                         "--train-periods", "14", "--test-periods", "8", "--methods", "equal_weight"])
        self.assertEqual(code, 2)
        result = json.loads((self.root/"bad/run.json").read_text())
        self.assertEqual(result["metrics"][0]["status"], "incomplete")

    def test_cli_bad_config_is_rejected_before_export(self):
        with redirect_stderr(StringIO()):
            self.assertEqual(main(["--synthetic", "--max-weight", ".1", "--output", str(self.root/"bad")]), 2)
        self.assertFalse((self.root/"bad").exists())


if __name__ == "__main__":
    unittest.main()
