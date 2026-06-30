# Cloud Lakehouse Design

## Storage Layout

Use clear zones:

```text
landing/
bronze/
silver/
gold/
quarantine/
checkpoints/
```

## Environment Layout

Use separate dev, test, and prod boundaries. Depending on the platform, this may mean separate workspaces, catalogs, buckets, resource groups, or accounts.

## Orchestration

Choose one primary orchestrator:

- Databricks Workflows for Databricks-native pipelines.
- Airflow for broader platform orchestration.
- ADF or cloud-native tools when the organization is Azure-centered.

## Observability

Track:

- Freshness.
- Runtime.
- Row counts.
- Failed records.
- Cost.
- SLA misses.
- Data quality violations.

## Disaster Recovery

- Keep raw data replayable.
- Define retention.
- Document backfill commands.
- Test restore and replay before an incident.

## Tradeoff

More isolation improves safety but increases operational overhead. More shared infrastructure reduces cost but increases blast radius.
