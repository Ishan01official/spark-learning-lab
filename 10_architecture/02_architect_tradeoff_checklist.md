# Architect Tradeoff Checklist

Use this checklist before proposing a design.

## Requirements

- What is the freshness SLA?
- What is the expected data volume?
- Is the source batch, streaming, or CDC?
- What happens when the source sends bad data?
- Who owns the data after ingestion?

## Storage

- Is raw data replayable?
- Are table names stable and understandable?
- Are partitions based on query patterns and file sizes?
- Is retention defined?
- Is schema evolution controlled?

## Compute

- Is the workload interactive, scheduled, or streaming?
- Should it run on job clusters or shared clusters?
- Is the cluster sized from measurement or guesswork?
- What is the fallback if autoscaling is slow?

## Orchestration

- What retries are safe?
- Which tasks are idempotent?
- How are backfills triggered?
- How are dependencies represented?

## Security And Governance

- Where is PII?
- Who can read Bronze, Silver, and Gold?
- Are service principals least-privilege?
- Is lineage needed for audits?

## Observability

- What metrics prove the pipeline is healthy?
- What alert fires when data is late?
- Where are driver and executor logs?
- What dashboard shows row counts and quality failures?

## Cost

- What is the largest cost driver?
- Can files be compacted?
- Can columns be pruned?
- Are dev/test/prod clusters right-sized?

## Interview Tip

Say the tradeoff explicitly:

"I would choose X because of Y. The downside is Z, so I would mitigate it with A."
