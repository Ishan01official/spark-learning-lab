# Project: dq-framework

A small, reusable data quality framework you can drop into any PySpark pipeline.

## What this project demonstrates

- A declarative `DQRule` API.
- Severity levels: `error` (fail the job) and `warn` (log only).
- Threshold support: fail only if more than X% of rows violate.
- A `DQReport` object that summarizes results and is JSON-serializable.
- Integration patterns: run-as-step vs run-inline, fail-fast vs quarantine.

## API

```python
from dq import DQRule, DQRunner

rules = [
    DQRule("amount_positive",  lambda df: F.col("amount") > 0, severity="error"),
    DQRule("status_valid",     lambda df: F.col("status").isin(VALID_STATUSES), severity="error"),
    DQRule("email_format",     lambda df: F.col("email").rlike(EMAIL_RE), severity="warn", threshold=0.01),
    DQRule("region_known",     lambda df: F.col("region").isin(KNOWN_REGIONS), severity="warn", threshold=0.05),
]

report = DQRunner(rules).run(df)
if report.has_errors():
    raise DQException(report.as_json())
print(report.as_json())
```

## What you get back

```json
{
  "total_rows": 1000000,
  "results": [
    {"name": "amount_positive", "severity": "error", "passed": true,  "violations": 0,    "rate": 0.0},
    {"name": "status_valid",    "severity": "error", "passed": true,  "violations": 0,    "rate": 0.0},
    {"name": "email_format",    "severity": "warn",  "passed": false, "violations": 12000, "rate": 0.012},
    {"name": "region_known",    "severity": "warn",  "passed": true,  "violations": 30000, "rate": 0.03}
  ],
  "has_errors": false,
  "has_warnings": true
}
```

## Files

- `dq/rules.py` — `DQRule`, `DQResult`, `DQReport`, `DQRunner`.
- `dq/quarantine.py` — helpers to split good/bad rows for the quarantine pattern.
- `run_demo.py` — runs the framework on a synthetic dataset.

## How to run

```bash
python run_demo.py
```

## Design choices

- **Predicate = `df -> Column`**. Same shape as filter predicates, so you can reuse them.
- **`passed = (violation_rate <= threshold)`**. Default threshold 0; tunable per rule.
- **No external state**. Pure functions over DataFrames.
- **JSON-serializable report** so it goes straight into CloudWatch / Datadog / Slack alerts.

## What it's NOT

- Not a profiler — there are libraries (Great Expectations, Soda) that do statistical drift detection.
- Not a schema validator — Delta CHECK constraints do that.
- Not a workflow tool — wrap it in whatever orchestration you use.

It's the minimal layer that gets ~80% of the value of the big frameworks.
