# Workspace, Jobs, And Clusters

## Simple Explanation

- Workspace: where users, notebooks, repos, jobs, and data assets live.
- Cluster: compute that runs Spark.
- Job: scheduled or triggered production work.
- Workflow: multiple tasks connected together.

## Production Explanation

Use all-purpose clusters for development and job clusters for production. Job clusters improve isolation and cost control because they start for the run and terminate after.

## Common Mistakes

- Running production jobs on shared interactive clusters.
- Installing libraries manually on a cluster and forgetting to pin them.
- Using a personal user identity for production jobs.
- Not setting retries or alerts.

## Checklist

- [ ] Job uses source-controlled code.
- [ ] Job cluster has pinned runtime.
- [ ] Libraries are pinned.
- [ ] Alerts are configured.
- [ ] Job owner is a service principal or production identity.
- [ ] Cluster policy prevents expensive accidental configs.

## Interview Answer

"For production Databricks jobs, I prefer job clusters, pinned runtime versions, cluster policies, source-controlled code, alerts, and service principal ownership. All-purpose clusters are better for exploration."
