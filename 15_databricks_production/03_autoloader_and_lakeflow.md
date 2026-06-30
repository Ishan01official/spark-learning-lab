# Auto Loader And Lakeflow Concepts

## Auto Loader

Auto Loader incrementally ingests new files from cloud object storage. It is useful when many files arrive over time and manual file listing becomes expensive or unreliable.

## Lakeflow Declarative Pipelines / Delta Live Tables Concepts

Managed declarative pipelines let you define tables, dependencies, expectations, and pipeline behavior while the platform handles orchestration details.

## When To Use

- Use Auto Loader for scalable cloud file ingestion.
- Use managed declarative pipelines when table dependencies and expectations are more important than custom orchestration code.
- Use normal Spark jobs when you need full control over runtime behavior.

## Common Mistakes

- Treating checkpoints as temporary.
- Changing source schema without planning evolution.
- Using one checkpoint path for multiple streams.
- Ignoring bad records.

## Interview Answer

"Auto Loader solves scalable incremental file discovery. Managed declarative pipelines solve table dependency and quality orchestration. I would still design Bronze/Silver/Gold boundaries and monitor freshness, quality, and cost."
