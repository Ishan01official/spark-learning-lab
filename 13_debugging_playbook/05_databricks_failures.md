# Databricks Production Failures

## Cluster Startup Issue

Symptoms:

- Job waits for cluster.
- Cluster fails to start.
- Library install timeout.

Check:

- Cluster policy.
- Node availability.
- Runtime version.
- Init scripts.
- Library resolution.

Fix:

- Use a supported runtime.
- Move fragile installs into tested wheels.
- Avoid slow init scripts.
- Use job clusters for isolated production runs.

## Permission Or Unity Catalog Issue

Symptoms:

- Table not found.
- Permission denied.
- Can access in notebook but job fails.

Check:

- Catalog, schema, and table grants.
- Service principal identity.
- Cluster access mode.
- External location permissions.

Fix:

- Grant least privilege to the production identity.
- Use fully qualified table names.
- Align cluster access mode with Unity Catalog requirements.

## Job Dependency Issue

Symptoms:

- Works interactively but fails as job.
- Missing module.
- Different package version.

Check:

- Job cluster libraries.
- Repo checkout path.
- Wheel version.
- Environment variables.

Fix:

- Package shared code.
- Pin dependencies.
- Use CI to validate imports.

## Interview Explanation

"Databricks failures often come from environment drift: identity, runtime, cluster policy, library versions, or access mode differ between dev and production."
