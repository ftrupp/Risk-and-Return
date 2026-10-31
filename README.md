# Risk and Return

An offline Python project for comparing portfolio construction rules under explicit
data, timing and transaction-cost assumptions. The reusable CLI and the English and
Spanish notebooks run the **same calculation engine**.

The problem is not just finding weights that look good in a historical sample.
It is making the estimation window, investable universe, constraints and subsequent
portfolio path consistent enough to evaluate those weights honestly.

## What it does

- Compares equal weights, capped minimum volatility and capped maximum Sharpe.
- Fits weights using training data only; enters at the next observed session close.
- Evaluates a fixed-unit, buy-and-hold portfolio through final liquidation.
- Calculates total return, calendar CAGR, annualized periodic mean, volatility,
  Sharpe and maximum drawdown from the same net wealth path.
- Validates a local CSV and explicit metadata, records excluded training assets and
  refuses to fill missing held prices in the test period.
- Exports weights, trajectories, metrics, costs, data-quality flags and input hashes.
- Includes a deterministic synthetic example that runs without network access.

There are no broker connections, orders, account credentials or downloads. This is
a research and teaching project, not a trading system or evidence of alpha.

## Install and run

Python 3.10+; validation was performed with Python 3.12. Use a virtual environment:

```sh
python -m venv .venv
# Activate .venv using the command appropriate for your shell.
python -m pip install -e .
python -m risk_return --synthetic --output outputs/synthetic
```

The installed entry point is also available as `risk-return`. Dependencies must be
installed first; the experiment itself is offline. A tested dependency snapshot is
provided in `requirements-tested.txt` for reproducing the numerical environment.

The default demonstration uses three invented assets, 252 training returns, 126
test returns, a 60% position cap, 10 basis points of costs on each entry/exit
notional, 10,000 simulated USD and an assumed constant 0% risk-free rate.
The synthetic paths are formulas, not historical observations or an empirical benchmark.

```sh
python -m risk_return --synthetic --cost-bps 25 --max-weight 0.5 --output outputs/cost_case
python -m risk_return --prices data/prices.csv --metadata data/metadata.json \
  --train-periods 252 --test-periods 126 --test-end 2025-03-07 \
  --methods equal_weight min_volatility --output outputs/local_case
```

Output directories must be new: previous runs are never overwritten. Exit code 0
means every requested strategy was evaluated; code 2 means invalid input or an
incomplete strategy. Check `metrics.csv` and `quality.json`, including failures.
Each run creates:

| File | Contents |
| --- | --- |
| `weights.csv` | Training decisions, before rounding; also retained if later valuation fails |
| `trajectory.csv` | Net wealth at common dates for evaluable strategies |
| `metrics.csv` | Results, training model estimates, status and reason |
| `costs.csv` | Entry/exit notional and proportional costs |
| `quality.json` | Eligibility, missing prices, window boundaries and verification limits |
| `run.json` | Configuration, data declarations, input hashes, versions and metrics |

See [the data contract](docs/DATA.md) before using observed data. Local data and
generated outputs are ignored by Git. The CLI accepts CSV/JSON only, never pickle.

## Method and accounting

The most recent requested window contains `train_periods + test_periods + 2` prices.
Training and test price blocks are disjoint. Weights are estimated at the last
training close and bought at the following close; the intervening movement is not
earned. A fixed `--test-end` and input hashes identify the evaluated sample.

Eligibility depends only on complete, positive training prices. There is no ranking
using test returns and no retrospective removal of a held asset with missing prices.
All eligible assets enter the equal-weight baseline; optimization uses the same set.

The model uses arithmetic annualized means and sample covariance. Constraints are
`sum(w)=1` and `0 <= w_i <= max_weight`. Minimum volatility minimizes `wᵀΣw`.
Maximum Sharpe minimizes the negative ratio, not its reciprocal; it requires
positive risk and a feasible portfolio with positive expected excess return.
Optimizer success and constraints are checked before accepting weights.

Costs are proportional to traded notional. With `c = cost_bps/10000`, entry notional
is `initial_capital/(1+c)` and final sale proceeds are terminal marked value times
`(1-c)`. The first wealth row is cash **immediately before entry**; the next observed
return includes entry cost, and the last includes exit cost. This convention keeps
all performance metrics on a single net path. It can affect short-sample volatility
and Sharpe. Costs are illustrative, not calibrated execution estimates.

After entry, adjusted units remain fixed; there is no periodic rebalancing, leverage
or external cash flow. Adjusted prices are total-return proxies, not an actual
share/dividend/cash ledger. The position cap applies at entry; market drift may
increase a later weight above that cap.

CAGR uses actual calendar days/365.25; it is not a weighted average of asset CAGR.
Volatility is sample standard deviation of net periodic returns times `sqrt(ppy)`.
Sharpe uses their mean excess over a constant periodic risk-free assumption,
annualized by `sqrt(ppy)`. Zero volatility gives an undefined Sharpe (`null`).
Arithmetic mean and CAGR remain separately labeled. Annualization does not prove
independence, normality or stability of returns.

## Reproducible demonstrations and checks

[English notebook](riskandreturn-EN.ipynb) · [Notebook en español](riskandreturn-ES.ipynb)

Both notebooks are compact, run sequentially from a fresh namespace, and contain
only freshly generated synthetic demonstration outputs. No private paths or market
cache is needed. For interactive use, install `python -m pip install -e ".[notebook]"`.

```sh
python -m unittest discover -s tests -v
python scripts/execute_notebooks.py            # Validate and execute both, network blocked
python scripts/execute_notebooks.py --write    # Regenerate their demonstration outputs
```

Two analytic checks make the meaning of the results concrete:

- With `Σ=diag(0.04,0.09)` and a 100% cap, minimum-volatility weights are
  `9/13, 4/13`; volatility is approximately `0.16641006`.
- With prices `A=[100,200,100]`, `B=[10,10,20]`, weights `25%/75%` and no costs,
  wealth from 1,000 is `1,000 → 1,250 → 1,750`: total return is 75%.

These are correctness checks, not investment results. The synthetic strategy
comparison is shown in the notebooks and can be regenerated with the CLI; its
ordering has no empirical significance. Tests also cover caps, optimizer failure,
fees, label/price-scale invariance, chronology, metadata and missing data.

## Limits and next work

An observed dataset may have survivorship bias, unavailable-at-the-time constituents,
revised prices or undocumented adjustments. Metadata are declarations, not independent
verification. The CLI does not certify exchange calendars, data licenses or tradability.
A historical cache used in an earlier notebook does not support a defensible claim
of live performance; it is not part of the current release.

No empirical out-of-sample superiority is claimed. Further work needs a predeclared
universe with delistings, verified total-return and currency treatment, comparable
benchmarks, historical cash returns, realistic execution costs and independent periods.
Parameter search and repeated use of a test window can create selection bias.

## Project history, references and license

This revision continues the original Risk and Return educational project and keeps
its existing [CC0-1.0 license](LICENSE). Standard mean-variance methods and third-party
libraries are not claimed as original algorithms. Method references are documented
in [REFERENCES.md](docs/REFERENCES.md).

The old in-notebook calculations, live download helpers and large pickle/checkpoint
files are retired from the current tree. Historical Git commits are preserved; the
old data may therefore remain in repository history. They are not a supported dataset
or covered by a new redistribution claim. [Migration notes](docs/MIGRATION.md) explain
the corrections and what changed.
