# Local data contract

CSV is UTF-8, comma-separated, with a `date` column followed by unique asset labels:

```csv
date,Asset_A,Asset_B
2024-01-02,100.0,50.0
2024-01-03,101.0,49.5
2024-01-04,100.5,50.2
```

This three-row example illustrates syntax only; a real experiment needs at least
`train_periods + test_periods + 2` rows. Dates must be unique, ordered YYYY-MM-DD
session dates without timezone/time components. Asset labels allow letters, digits,
dots, underscores and hyphens, starting with a letter/digit, up to 32 characters.
No formulas, duplicate headers or extra CSV fields are accepted.

Prices must be positive finite total-return adjusted closes, in one declared currency
per adjusted unit. The tool does not perform FX conversion or create adjustments.
Blank numeric fields mean missing observations. They exclude an asset if in training
and invalidate a strategy if it holds that asset during a missing test observation.
Dates and prices are never silently filled, sorted or dropped. Zero/negative prices
and infinity are rejected. Adjusted units are not literal executable share counts.

The accompanying JSON must contain every field below. This is an explanatory
template, not observed data; replace the text, dates and declarations truthfully:

```json
{
  "schema_version": 1,
  "dataset_id": "your-versioned-dataset-id",
  "kind": "observed",
  "currency": "USD",
  "unit": "currency_per_adjusted_unit",
  "price_basis": "total_return_adjusted_close",
  "frequency": "session_daily",
  "periods_per_year": 252,
  "coverage_start": "2024-01-02",
  "coverage_end": "2024-01-04",
  "source": "Provider URL or a documented local provenance reference",
  "publication_date": null,
  "retrieved_at": "2024-01-05",
  "license": "Record the provider's actual terms; do not assume CC0 for market data",
  "point_in_time_universe": null,
  "dividends_included": true,
  "limitations": ["Publication date and historical universe not independently verified"]
}
```

Coverage dates must match the CSV. `retrieved_at` is required for observed data;
`publication_date` may be null when unknown. Unknown universe availability is null,
not true. Total-return prices require an affirmative dividends declaration; if that
cannot be established, prepare the data before claiming it fits this contract.
The generated synthetic example declares no external publication/retrieval and uses
invented simulated USD prices without separate dividends or exchange holidays.

The runner records metadata and hashes locally. It cannot verify a provider's
statements. Do not distribute third-party data unless its terms permit it. No market
dataset, account data or credentials are bundled. Keep private input and outputs
outside tracked source; `.gitignore` excludes `data/` and `outputs/` by default.
