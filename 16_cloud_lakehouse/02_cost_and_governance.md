# Cost And Governance

## Cost Levers

- Right-size clusters.
- Use job clusters and auto-termination.
- Avoid unnecessary shuffles.
- Compact small files.
- Partition only when it helps.
- Use column pruning and predicate pushdown.
- Track expensive jobs.

## Governance Levers

- Catalog and schema ownership.
- Least-privilege permissions.
- PII classification.
- Data contracts.
- Lineage.
- Audit logs.
- Change management.

## Common Mistake

Optimizing compute while ignoring storage layout. Bad table design can keep costs high even on good clusters.

## Interview Answer

"I optimize cost by measuring where compute time is spent, reducing unnecessary data movement, right-sizing ephemeral clusters, and fixing file layout. I optimize governance by defining ownership, permissions, PII controls, contracts, and lineage."
