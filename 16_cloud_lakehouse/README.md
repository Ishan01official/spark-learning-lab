# 16_cloud_lakehouse - Cloud, Governance, Cost, And Platform Architecture

This module is for platform-level data engineering.

## Topics

- Spark on cloud.
- Data lake architecture.
- Lakehouse architecture.
- Storage layout.
- IAM and security basics.
- Cost optimization.
- Multi-environment design.
- Dev/test/prod setup.
- Orchestration with Airflow, ADF, or Databricks Workflows.
- Observability.
- Data contracts.
- Governance.
- Lineage.
- Disaster recovery.
- SLA/SLO thinking.
- Architect-level tradeoffs.

## Notes

1. [`01_cloud_lakehouse_design.md`](./01_cloud_lakehouse_design.md)
2. [`02_cost_and_governance.md`](./02_cost_and_governance.md)

## Mental Model

Cloud lakehouse architecture is about separating concerns:

- Storage is durable and cheap.
- Compute is elastic and temporary.
- Metadata governs discoverability.
- Permissions protect data.
- Orchestration controls dependency.
- Observability proves health.
