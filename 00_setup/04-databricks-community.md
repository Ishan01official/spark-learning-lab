# 04 — Databricks Community Edition

## Why this matters

A free, hosted Spark + Delta environment with a notebook UI. Two reasons to use it:

1. You hit a wall locally (laptop is too small, want to try a real cluster).
2. You're preparing for the **Databricks Certified Associate Developer** exam, which is graded against the Databricks UI / runtime.

It's free forever, no credit card. You get a tiny single-node cluster (~15 GB RAM) that auto-terminates after ~2h of inactivity. Perfect for learning, useless for production.

## Sign up

1. Go to <https://www.databricks.com/learn/free-edition> (formerly "Community Edition" — same product, the marketing name keeps changing). Click **Get Started Free**.
2. Pick **Community Edition** at the bottom of the cloud-provider screen — *do not* pick AWS/Azure/GCP unless you want to be billed.
3. Verify email, log in.

## Create your first cluster

1. Sidebar → **Compute** → **Create compute**.
2. Pick the default runtime (Databricks Runtime 14.x LTS or newer). It bundles PySpark, Delta, and a bunch of libraries.
3. Name it `learn-cluster`. Click **Create**.

You can only have one running cluster at a time on Community Edition. That's fine.

## Create a notebook

1. Sidebar → **Workspace** → your user → **Create** → **Notebook**.
2. Default language: **Python**.
3. Attach it to `learn-cluster`.

## Run your first cell

```python
df = spark.range(1_000_000)
df.count()
```

That should print `1000000`. You're done.

Note: in a Databricks notebook, `spark` already exists — you don't create a SparkSession. In a local `.py` script you do. This is the only meaningful syntactic difference between local PySpark and Databricks PySpark.

## Useful tricks

- `%fs ls /databricks-datasets/` — Databricks ships ~20 GB of sample datasets. Great for the rest of this curriculum.
- `display(df)` — Databricks-specific. Renders an interactive table with sort/filter and lets you chart. Not in vanilla PySpark.
- `%sql` magic — flip a cell to SQL: `%sql SELECT * FROM range(10)`.
- Cluster Spark UI — Compute → click cluster → **Spark UI** tab. Same UI as local, just hosted.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| "Your trial has expired" banner | Sign-up flow accidentally picked AWS/Azure | Re-register and pick Community Edition explicitly. |
| Cluster won't start | Free quota is exhausted globally — Databricks rate-limits free tier. | Wait a few minutes and retry. |
| `display()` is not defined | You ran code outside a Databricks notebook. | Use `.show()` in `.py` scripts. |

## Mapping local code to Databricks code

| Local | Databricks |
|---|---|
| `spark = SparkSession.builder.master("local[*]")...getOrCreate()` | `spark` already exists. Skip this. |
| `spark.read.csv("data/users.csv")` | `spark.read.csv("/databricks-datasets/.../users.csv")` |
| `df.show(20, False)` | `display(df)` (or `.show()`, both work) |
| Spark UI at `localhost:4040` | Cluster page → **Spark UI** tab |

## References

- [LS Ch.2 §"Using the Databricks Community Edition"]
- [Databricks Free Edition docs](https://docs.databricks.com/aws/en/getting-started/community-edition)
- 📺 [Getting Started with Databricks Community Edition — Databricks Academy](https://www.databricks.com/learn/training/lp/apache-spark-developer-essentials)
