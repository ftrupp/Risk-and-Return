# Migration from the original notebooks

The numerical implementation now lives in `risk_return/`. Run the CLI for local
analysis; notebooks demonstrate the same API using deterministic synthetic data.
There is one supported engine for both languages.

Corrections retained and extended from the Spanish notebook audit:

- Candidate weights appear on both sides of the covariance quadratic form.
- Returns, volatility and Sharpe derive from a single portfolio path and date range.
- Calendar CAGR is distinct from arithmetic annualized mean; asset CAGR is not summed.
- Negative Sharpe replaces the singular reciprocal objective. Infeasible positive
  excess, degenerate risk or unsuccessful solver outcomes are explicit failures.
- Weights are selected with training information only, then entered at the next close.
- A constant risk-free assumption is declared instead of applying today's yield to history.
- Missing test prices never silently change the held universe or create partial wealth.
- Proportional entry/exit costs and a maximum initial position size are now configurable.

The old English notebook was replaced too, so a known-incorrect variant is not left
beside the corrected implementation. Old outputs were removed and new demonstrations
were generated from the synthetic dataset only.

Live-download helpers and their implicit installation/network/file-writing behavior
are no longer part of the release. Legacy pickle files, compiled bytecode and notebook
checkpoints are removed from the tracked current tree, not deleted from the original
workstation. Repository history is not rewritten. The prior historical data are not
an officially supported or newly licensed dataset.

This release is an accounting and reproducibility improvement, not evidence of alpha.
It does not replicate an earlier historical return claim or certify the old cache.
