# 05 — Production deployment patterns

## Why this matters

The same PySpark script runs differently in `python my_job.py` vs spark-submit vs Databricks vs EMR vs Glue. This note covers the patterns that make a Spark job *deployable*: project layout, dependency management, configuration, and the four ways to actually run it.

## Project layout

A reasonable PySpark project:

```
my_pipeline/
├── pyproject.toml            # deps, build config
├── src/
│   └── my_pipeline/
│       ├── __init__.py
│       ├── config.py         # env-driven config
│       ├── jobs/
│       │   ├── __init__.py
│       │   ├── ingest_orders.py
│       │   └── build_silver_orders.py
│       ├── lib/
│       │   ├── delta_helpers.py
│       │   ├── data_quality.py
│       │   └── logging.py
│       └── schemas/
│           └── orders.py
├── tests/
│   ├── unit/
│   └── integration/
└── deploy/
    ├── Dockerfile
    ├── databricks/
    │   └── workflows.yml
    └── airflow/
        └── dags/
```

Key points:
- `src/` layout so tests can't import accidentally; install with `pip install -e .`.
- Jobs are thin: parse args → call library functions → write.
- Libraries are testable: pure functions, take DataFrame in, return DataFrame out.

## Dependency management

Three options, in order of how much you should prefer them:

### 1. A pyproject.toml + wheel

```toml
[project]
name = "my-pipeline"
version = "0.3.2"
dependencies = [
    "pyspark>=3.5,<4.0",
    "delta-spark>=3.2",
    "pydantic>=2.0",
]
```

Build a wheel (`pip wheel . -w dist/`), include it in your spark-submit. Standard, works everywhere.

### 2. Conda / venv tarball

For environments where the cluster has Internet access but you want a specific Python version:

```
conda env create -n my-pipeline python=3.11
conda activate my-pipeline
pip install -r requirements.txt
conda pack -o env.tar.gz
```

Ship `env.tar.gz` to S3, point `spark.archives` at it.

### 3. PEX / shiv

Single-file executables for closed networks. Heavyweight; only use when (1) and (2) don't fit.

## Configuration

Don't hard-code paths, table names, or credentials. The canonical pattern:

```python
from pydantic import BaseSettings

class JobConfig(BaseSettings):
    env: str  # "dev" / "stg" / "prod"
    bronze_path: str
    silver_path: str
    batch_id: str

    class Config:
        env_prefix = "PIPELINE_"

cfg = JobConfig()  # reads PIPELINE_ENV, PIPELINE_BRONZE_PATH, etc.
```

Or for Spark-specific configs, use a YAML file per env:

```yaml
# config/prod.yaml
bronze_path: s3://prod-lake/bronze/
silver_path: s3://prod-lake/silver/
shuffle_partitions: 800
broadcast_threshold: 100mb
```

Load at startup, never bake into code.

## Logging (cribbing from module 02 — same conventions as Ishan's Lambdas)

```python
import logging
import json
import sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)
        return json.dumps(payload)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

log = logging.getLogger("my_pipeline")
log.info("starting batch", extra={"extra_fields": {"batch_id": cfg.batch_id}})
```

Structured logs → ingest into CloudWatch / Datadog / Splunk → searchable.

## Four ways to run

### 1. spark-submit (canonical)

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.executor.memory=8g \
    --conf spark.executor.cores=4 \
    --conf spark.executor.instances=20 \
    --conf spark.sql.shuffle.partitions=800 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --py-files dist/my_pipeline-0.3.2-py3-none-any.whl \
    src/my_pipeline/jobs/build_silver_orders.py \
    --bronze-path s3://lake/bronze/orders \
    --silver-path s3://lake/silver/orders
```

Bare metal / on-prem / older EMR. Verbose but explicit.

### 2. Databricks Jobs / Workflows

A YAML or JSON job definition:

```yaml
# deploy/databricks/workflows.yml
resources:
  jobs:
    silver_orders:
      name: silver_orders
      tasks:
        - task_key: build
          notebook_task:
            notebook_path: /pipelines/build_silver_orders
            base_parameters:
              env: prod
          existing_cluster_id: ${var.cluster_id}
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: UTC
```

Then `databricks bundle deploy`. The Databricks Asset Bundle CLI does the right thing for IaC.

### 3. AWS Glue

PySpark scripts uploaded to S3, jobs defined in Glue with `--py-files` and `--conf` analogues. Glue auto-manages the cluster but with constraints on Spark version and packages. Use the AWS-provided `awsglue` library for catalog integration.

### 4. Airflow + SparkSubmitOperator (or KubernetesPodOperator)

For schedule orchestration. Airflow doesn't run Spark; it launches the spark-submit / Kubernetes Job that does.

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

silver = SparkSubmitOperator(
    task_id="silver_orders",
    application="s3://artifacts/silver_orders.py",
    conn_id="emr_spark",
    conf={"spark.sql.shuffle.partitions": "800"},
    application_args=["--batch-id", "{{ ds }}", "--env", "prod"],
)
```

## Testing strategy

| Test type | What | How |
|---|---|---|
| Unit | Pure functions taking DF → DF | `pytest` + `pyspark.sql.SparkSession` per test class |
| Schema | Tables have expected columns/types | `assert df.schema == expected_schema` |
| Data-level | Specific input produces specific output | Small in-memory fixtures |
| Integration | End-to-end on a tiny dataset | Spawn local Spark, run the job, assert outputs |
| Production smoke | Job runs in prod cluster for ~30s with sample | Run once before deploying |

Avoid the trap of testing the framework — focus tests on your business logic.

## Failure handling

In production, jobs fail. Patterns:

- **Idempotent retries**: see note 02. Always assume your job will run twice.
- **Dead-letter for unparseable inputs**: never silently drop.
- **Bounded retries with backoff**: Airflow handles this; don't write your own loop.
- **Failure runbook per job**: list of "what to do if this errors". Linked from the alert.

## Deployment cadence

A reasonable workflow:

1. Develop locally; tests pass.
2. CI builds wheel, runs unit + integration tests.
3. Push to staging; run on a sample, validate output.
4. Push to prod; run on full dataset.
5. Monitor first run; rollback procedure ready.

For Spark jobs, "rollback" usually means redeploying the previous wheel/JAR. For Delta-modified tables, also `RESTORE TO VERSION` if the job's outputs are incorrect.

## Monitoring

What to alert on:
- **Job failure** (obvious).
- **Job ran but produced 0 rows** (silent failure).
- **Job ran but produced unexpected row count** (statistical alert).
- **Job duration exceeded SLA** (cost / latency drift).
- **Data quality threshold violated** (see note 03).
- **Storage growth anomaly** (a 10× partition could be a bug).

## References

- [HPS Ch.11] — "Productionalizing Spark"
- *Fundamentals of Data Engineering* — Ch.10–11 on ops
- Databricks Asset Bundles: https://docs.databricks.com/en/dev-tools/bundles/
- AWS Glue PySpark: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html
- 📺 [Productionizing Apache Spark — Holden Karau](https://www.youtube.com/results?search_query=productionizing+apache+spark+holden+karau)
