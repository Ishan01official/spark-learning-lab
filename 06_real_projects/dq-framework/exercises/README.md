# dq-framework exercises

---

## 1. Add a uniqueness rule

Extend `DQRule` to support row-level uniqueness checks:

```python
DQRule.unique("order_id", severity="error")
```

This is *not* a per-row predicate — it requires aggregation. How do you fit it into the same framework cleanly?

---

## 2. Add a foreign-key check

Add a rule type that takes a reference DataFrame:

```python
DQRule.foreign_key("customer_id", reference_df=customers, ref_col="customer_id")
```

Check that every value in the input's `customer_id` exists in `customers.customer_id`. Implement as a left-anti join.

---

## 3. Add a statistical rule

Add a rule that fails if the row count is more than ±X% from a historical mean:

```python
DQRule.row_count_within(expected=1000000, tolerance=0.20)
```

Where does "expected" come from? (A metadata table that the pipeline updates each run.)

---

## 4. Sample-then-exact

For expensive rules (e.g. regex matching), check first on a 10% sample. If that passes, skip the full check. If it fails, run the full check to get the exact violation rate.

Implement this as a `DQRule.sample_first(...)` decorator.

---

## 5. JSON schema-based rules

Let the rules be defined in a YAML/JSON file rather than Python:

```yaml
rules:
  - name: amount_positive
    severity: error
    expr: "amount > 0"
  - name: status_valid
    severity: error
    expr: "status IN ('NEW','PAID','SHIPPED','REFUNDED')"
```

Build a loader that parses this and emits `DQRule` objects. Now ops can edit DQ without redeploying Python.

---

## 6. Integrate with a job

Take `orders-etl/02_build_silver.py` and add a DQ step that runs *before* the MERGE. Errors should:
- Print the report.
- Quarantine bad rows to the dead-letter table.
- Fail the job ONLY if error-rule violation count is > 0.

Warnings should be logged but not fail.
