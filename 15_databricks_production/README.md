# 15_databricks_production - Managed Spark In Production

This module maps Spark concepts to Databricks production work.

## Why This Matters

Databricks is not just "Spark with notebooks." Production work includes jobs, workflows, clusters, policies, repos, Unity Catalog, secrets, service principals, monitoring, runtime upgrades, and cost control.

## Topics

- Workspace basics.
- Clusters.
- Jobs and Workflows.
- Notebooks and Repos.
- DBFS and cloud storage.
- Unity Catalog.
- Access modes.
- Cluster policies.
- Job clusters vs all-purpose clusters.
- Databricks Runtime.
- Photon.
- Lakeflow Declarative Pipelines / Delta Live Tables concepts.
- Auto Loader.
- Secrets.
- Service principals.
- CI/CD basics.
- Monitoring Databricks jobs.
- Common production failures.

## Notes

1. [`01_workspace_jobs_clusters.md`](./01_workspace_jobs_clusters.md)
2. [`02_unity_catalog_security.md`](./02_unity_catalog_security.md)
3. [`03_autoloader_and_lakeflow.md`](./03_autoloader_and_lakeflow.md)

## Production Rule

Development can be notebook-first. Production should be identity-aware, version-controlled, observable, repeatable, and cost-governed.
