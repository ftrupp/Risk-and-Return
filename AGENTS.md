# Preserve the authored notebooks

The user requires the full English and Spanish notebooks to remain at
`riskandreturn-EN.ipynb` and `riskandreturn-ES.ipynb`. Preserve their explanations,
experiments, code, figures and tables. Do not replace them with short examples or
remove sections during refactoring unless the user explicitly requests that scope.

Keep reusable Python code and synthetic demonstrations as additional artifacts.
The `riskandreturn-offline-*.ipynb` files are examples, not substitutes for the full
studies. `scripts/execute_notebooks.py` must target only these offline examples.

When reporting validation, distinguish the complete studies from the offline
examples. Passing package tests or running examples does not validate the full
notebooks. Correct the full notebooks in place when requested, retaining their
scope and recording the cells and calculations actually checked.
