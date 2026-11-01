# Original notebooks and the companion engine

The complete authored notebooks remain at their original paths:

- `riskandreturn-EN.ipynb`: 80 cells, including 36 code cells.
- `riskandreturn-ES.ipynb`: 92 cells, including 36 code cells.

They were restored exactly from the files preceding commit `beef9896fde9`, whose
replacement of the full studies with short examples was not the intended outcome.
All original narrative, code, experiments, charts, outputs and metadata are retained.
The original `data_fetcher.py`, `financial_instruments.py` and `links_indices.json`
are included again. Existing local data and checkpoints are preserved, but those
large files are still excluded from the tracked current tree.

The reusable implementation in `risk_return/` and its CLI remain available. Their
small synthetic demonstrations are separate, additional files:

- `riskandreturn-offline-EN.ipynb`
- `riskandreturn-offline-ES.ipynb`

`scripts/execute_notebooks.py` executes only these offline demonstrations. Its
`--write` option never rewrites the complete original notebooks.

## Validation scope

The full notebooks were verified against both the pre-change backups and the
historical Git revision, including every cell and saved output. They were not
re-executed or numerically recertified during restoration. Their original data and
dependencies are required for a fresh run; the preserved outputs are historical.

The prior 35 passing tests and successful execution of seven code cells per
language apply to the companion engine and its short offline examples. They do
not establish that all cells in the original notebooks run successfully.

The companion engine includes corrections to the covariance quadratic form,
portfolio return accounting, CAGR, Sharpe objective, training/test separation,
missing-price handling and transaction costs. Restoring the original notebooks
does not silently replace their historical calculations with this different engine.
Future corrections to the full studies must preserve their narrative and analysis,
and be validated against the full notebooks themselves.

Repository history is not rewritten. The historical data are not an officially
supported or newly licensed dataset. Neither restoration nor the synthetic
demonstrations establish empirical investment performance.
