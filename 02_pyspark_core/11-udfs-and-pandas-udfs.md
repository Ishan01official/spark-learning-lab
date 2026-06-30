# 11 — UDFs and Pandas UDFs

## Why this matters

Custom Python functions look harmless but can be the slowest line in your job. The default "row-at-a-time" UDF serializes every row from JVM → Python → JVM, blocks Catalyst optimizations, and is typically 10–100× slower than a built-in. **Vectorized (Pandas) UDFs** fix most of that.

## When to use a UDF at all

In order of preference:
1. **Built-in function** — always first choice (see note 06).
2. **SQL expression** via `F.expr(...)` — for things like `F.expr("aes_decrypt(payload, key)")`.
3. **Pandas UDF** — when no built-in exists and the work is non-trivial.
4. **Row-at-a-time UDF** — last resort, simple logic on small data.

Reality check: ~90% of UDFs in real code are reinventions of `regexp_extract`, `when/otherwise`, or `coalesce`. Look hard before reaching for `udf`.

## Row-at-a-time UDF (the slow kind)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def normalize_phone(p):
    if p is None:
        return None
    return "".join(ch for ch in p if ch.isdigit())

df.withColumn("phone_clean", normalize_phone("phone")).show()
```

What happens under the hood:
1. Each row's `phone` value is serialized in the JVM executor.
2. Sent over a socket to a Python worker process.
3. Python evaluates `normalize_phone`.
4. Result serialized back to JVM.
5. Repeat ×N rows.

Catalyst cannot see inside the Python — predicate pushdown and other optimizations stop at the UDF boundary.

## Pandas UDF (the fast kind) — vectorized

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

@pandas_udf(StringType())
def normalize_phone(p: pd.Series) -> pd.Series:
    return p.str.replace(r"\D", "", regex=True)

df.withColumn("phone_clean", normalize_phone("phone")).show()
```

Now Spark sends **batches** (Arrow-encoded record batches, default 10 000 rows) of `phone` to Python at a time. Pandas runs vectorized C operations. Result batch shipped back. Typically **5–50×** faster than the row-at-a-time version.

### Pandas UDF flavors

| Type signature | What it does | Use for |
| --- | --- | --- |
| `Series → Series` | one column in, one column out, same length | scalar transforms (the common case) |
| `Iterator[Series] → Iterator[Series]` | streaming over batches; can keep state (model load) | expensive setup per worker |
| `Series → scalar` | aggregate within a group | replace UDAF |
| `(Series, Series, ...) → Series` | multi-arg scalar | combine columns |
| `Iterator[Tuple[Series,...]] → Iterator[Series]` | streaming multi-arg | as above with state |

### Loading a model once per worker (Iterator pattern)

```python
import pickle
from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def predict(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    with open("/dbfs/models/v1.pkl", "rb") as f:        # loaded ONCE per executor
        model = pickle.load(f)
    for batch in batches:
        yield pd.Series(model.predict(batch.values))
```
Loading inside `predict` (without iterator) would deserialize the model *per batch*. The iterator pattern is the difference between 10 minutes and 10 hours.

## `mapInPandas` — per-batch arbitrary transform

When you want full pandas semantics on each batch (e.g. apply a sklearn transformer that returns multiple columns):

```python
def featurize(iterator):
    for pdf in iterator:                                 # pdf: pandas DataFrame
        pdf["x_norm"] = (pdf["x"] - pdf["x"].mean()) / pdf["x"].std()
        pdf["y_norm"] = (pdf["y"] - pdf["y"].mean()) / pdf["y"].std()
        yield pdf

out = df.mapInPandas(featurize, schema="id LONG, x DOUBLE, y DOUBLE, x_norm DOUBLE, y_norm DOUBLE")
```
**Warning:** normalizing per *batch* is statistically wrong if you wanted dataset-wide stats. For dataset-wide ops, compute the stats first in Spark, then apply.

## `applyInPandas` — per-group pandas

Apply a pandas function to all rows of each group at once. Useful for group-wise model fitting, complex windowing, or sklearn transformers.

```python
def fit_per_country(pdf: pd.DataFrame) -> pd.DataFrame:
    # pdf is one country's full data
    from sklearn.linear_model import LinearRegression
    X = pdf[["x1", "x2"]].values
    y = pdf["y"].values
    model = LinearRegression().fit(X, y)
    return pd.DataFrame([{
        "country": pdf["country"].iloc[0],
        "coef1": model.coef_[0],
        "coef2": model.coef_[1],
        "intercept": model.intercept_,
    }])

out = (df.groupBy("country")
         .applyInPandas(fit_per_country,
                        schema="country STRING, coef1 DOUBLE, coef2 DOUBLE, intercept DOUBLE"))
```

Each group must fit in the executor's memory (one task = one group). For huge groups, you can't use this.

## Configuration

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```
Arrow is on by default in Spark 3.x. If you see warnings about Arrow being disabled, install `pyarrow` and check the conf.

## Industry use cases

| Use case | Best approach |
| --- | --- |
| Clean phone / email / postal codes | Pandas UDF (`str.replace`) |
| Apply pre-trained ML model to billions of rows | Pandas UDF Iterator pattern + Arrow |
| Per-country forecasting model fit | `applyInPandas` group-wise |
| Decrypt payloads with a per-row IV | Pandas UDF — built-in `aes_decrypt` if scheme matches |
| Geo distance between two columns | Built-in `ST_DistanceSphere` (with Sedona); otherwise Pandas UDF using `geopy` |

## Scale notes

| Function | 1M rows | 1B rows |
| --- | --- | --- |
| Built-in `regexp_replace` | 5–10 s | 5–10 min |
| Pandas UDF (vectorized) | 15–30 s | 15–30 min |
| Row-at-a-time UDF | 2–5 min | several hours |

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| Job is suddenly 50× slower after adding a UDF | exactly what you'd expect | switch to Pandas UDF or built-in |
| `PythonException` mid-stage | exception in UDF on a specific row | log & skip in the UDF; or fix the row |
| `pyarrow.lib.ArrowInvalid: Could not convert ... with type ...` | mismatch between declared returnType and what Pandas returned | match return dtype to `returnType` |
| Iterator UDF loads model per batch | model load is inside the `for` loop | move it outside the loop |
| `applyInPandas` OOMs on a giant group | one group exceeds executor memory | salt or stratify the group key |
| UDF result has wrong nulls | UDF returns Python `None` for non-nullable column | declare nullable, or guard inside UDF |

## A debugging tip

Wrap a UDF body in try/except and log to a column so you can see *which rows* fail:

```python
@pandas_udf("string")
def safe_normalize(p: pd.Series) -> pd.Series:
    out = []
    for v in p:
        try:
            out.append("".join(ch for ch in (v or "") if ch.isdigit()) or None)
        except Exception as e:
            out.append(f"ERR: {e}")
    return pd.Series(out)
```

## References

- 📚 [LS Ch.5 §"User-Defined Functions"]
- 📚 [HPS Ch.5 §"Vectorized UDFs"]
- 📚 [DAS Ch.6 §"Pandas UDFs"]
- 📺 [PyArrow + Spark Pandas UDF — Databricks tutorials](https://www.youtube.com/results?search_query=pandas+udf+pyspark+arrow)
- 📺 ["Stop using PySpark UDFs"](https://www.youtube.com/results?search_query=pyspark+stop+using+udfs)
