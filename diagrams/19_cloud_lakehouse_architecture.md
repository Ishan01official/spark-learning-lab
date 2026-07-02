# 19 — Cloud Lakehouse Architecture

## Why this matters

This is the architect-level view: how Spark, Delta, and Databricks-style compute fit into a whole
cloud platform, with governance, orchestration, and consumers around them. System-design
interviews for senior data roles are essentially "draw this diagram and defend each box".

## The idea in plain English

The lakehouse bet is simple: **store everything once, in open formats, on cheap object storage;
bring different compute engines to that one copy.** Storage and compute scale (and bill)
independently. A governance layer sits across everything so one set of permissions and lineage
covers ETL, SQL, BI, and ML. Around the core: orchestration triggers work, observability watches
it, and consumers read from serving layers.

## Diagram — the layered platform

```mermaid
flowchart TB
    subgraph SRC["Sources"]
        OLTP["Operational DBs (CDC)"]
        EVT["Events (Kafka / Kinesis / Event Hubs)"]
        SAAS["SaaS APIs, files, partners"]
    end

    subgraph ING["Ingestion"]
        CDC["CDC tools / Kafka Connect"]
        AL["Spark: Auto Loader + Structured Streaming"]
    end

    subgraph STORE["Storage — one copy, open format"]
        OBJ["Object storage: S3 / ADLS / GCS"]
        DELTA["Delta / Iceberg tables<br/>bronze → silver → gold"]
        OBJ --- DELTA
    end

    subgraph COMPUTE["Compute — many engines, same tables"]
        ETL["Spark jobs (ETL, streaming)"]
        SQLW["SQL warehouse (BI queries)"]
        MLC["ML training / feature pipelines"]
    end

    subgraph GOV["Governance (across everything)"]
        CAT["Catalog: permissions, lineage, audit<br/>e.g. Unity Catalog"]
        DQ["Data quality + contracts"]
    end

    subgraph CONS["Consumers"]
        BI["BI dashboards"]
        DS["Data science / notebooks"]
        APPS["APIs, reverse ETL, sharing"]
    end

    ORCH["Orchestration: Workflows / Airflow"]
    OBS["Observability: run alerts,<br/>cost, freshness SLAs"]

    SRC --> ING --> STORE
    STORE <--> COMPUTE
    COMPUTE --> CONS
    GOV -.-> STORE
    GOV -.-> COMPUTE
    GOV -.-> CONS
    ORCH -.-> COMPUTE
    OBS -.-> COMPUTE
```

## Diagram — why lakehouse (the two-copy problem it kills)

```mermaid
flowchart LR
    subgraph OLD["Before: lake + warehouse"]
        L1["Data lake<br/>cheap, ML-friendly,<br/>no ACID, low trust"]
        W1["Warehouse<br/>ACID, fast SQL,<br/>expensive, proprietary"]
        L1 -->|"duplicate ETL,<br/>drift, double cost"| W1
    end

    subgraph NEW["Lakehouse"]
        ONE["One copy: open table format on object storage<br/>ACID via transaction log (page 10)<br/>SQL, ETL, streaming, and ML on the same tables"]
    end

    OLD -->|"consolidates into"| NEW
```

## Cloud mapping cheat sheet

| Layer | AWS | Azure | GCP |
| --- | --- | --- | --- |
| Object storage | S3 | ADLS Gen2 | GCS |
| Streaming ingest | Kinesis / MSK | Event Hubs | Pub/Sub |
| Spark compute | Databricks / EMR / Glue | Databricks / Synapse / Fabric | Databricks / Dataproc |
| BI SQL | Databricks SQL / Athena / Redshift | Databricks SQL / Fabric | Databricks SQL / BigQuery |
| Orchestration | Workflows / MWAA / Step Functions | Workflows / ADF | Workflows / Composer |

The architecture is cloud-agnostic; only the box labels change — which is precisely the argument
for open table formats.

## Key takeaways

- **Separation of storage and compute** is the economic foundation: storage is pennies, compute is
  dollars, and idle compute can be zero.
- The catalog/governance layer is what makes "one copy" safe — without central permissions and
  lineage, the lakehouse is just a data swamp with better file formats.
- Design for **egress of trust, not egress of data**: consumers read governed gold tables (or
  Delta Sharing), not copies emailed around.
- Cost levers, in order: right-size and auto-terminate compute, incremental instead of full
  recompute, file compaction, then storage tiering —
  [`16_cloud_lakehouse/02_cost_and_governance.md`](../16_cloud_lakehouse/02_cost_and_governance.md).
- DR is mostly a storage problem: replicate buckets + metadata; compute is recreated from code
  (which is why everything-as-code matters, [page 18](./18_databricks_production_workflow.md)).

## Common mistakes

- Rebuilding the two-copy world *inside* the lakehouse: exporting gold tables into a separate
  warehouse "for BI" out of habit.
- Governance as an afterthought — retrofitting permissions and lineage onto hundreds of tables is
  a year-long project; starting with a catalog is a day.
- One giant cluster for everything instead of per-workload compute (ETL jobs vs BI warehouse vs
  ML), which is the whole point of separated compute.
- Ignoring data contracts at the edges: the platform is only as reliable as the schemas sources
  promise.

## Interview angle

The system-design prompt: *"Design an analytics platform for a company with operational DBs,
clickstream, and ML needs."* Draw the layered diagram, then defend: why object storage + open
format (cost, no lock-in, multi-engine), why a central catalog (governance), where streaming vs
batch fits ([page 14](./14_batch_vs_streaming.md)), and how you'd control cost. Trade-off talk
beats technology name-dropping.

## Related in this repo

- [`16_cloud_lakehouse/01_cloud_lakehouse_design.md`](../16_cloud_lakehouse/01_cloud_lakehouse_design.md)
- [`16_cloud_lakehouse/02_cost_and_governance.md`](../16_cloud_lakehouse/02_cost_and_governance.md)
- [`10_architecture/01_lakehouse_system_designs.md`](../10_architecture/01_lakehouse_system_designs.md)
- [`10_architecture/02_architect_tradeoff_checklist.md`](../10_architecture/02_architect_tradeoff_checklist.md)
- [`04_delta_lake/09-format-comparison.md`](../04_delta_lake/09-format-comparison.md) — Delta vs Iceberg vs Hudi
