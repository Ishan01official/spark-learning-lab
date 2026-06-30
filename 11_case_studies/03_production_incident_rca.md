# Case Study - Production Incident RCA

## Incident

The daily Gold revenue table was two hours late and downstream dashboards missed the business SLA.

## Timeline

- 01:00 - Bronze ingestion started.
- 01:20 - Silver job started.
- 01:35 - Silver join stage slowed down.
- 03:50 - Job failed with executor OOM.
- 04:10 - Manual rerun started with larger cluster.
- 05:00 - Gold table completed.

## Symptoms

- One join stage dominated runtime.
- A few tasks were much slower than the rest.
- Shuffle spill increased.
- Executor OOM appeared in logs.

## Root Cause

A new upstream default customer id created a hot key. The Silver customer join became skewed.

## Fix

- Quarantined records with invalid default customer id.
- Added skew detection check before the join.
- Added AQE skew join settings.
- Added incident query to monitor top key distribution.

## Prevention

- Validate business keys before wide joins.
- Alert on high quarantine rates.
- Track task duration skew in critical jobs.
- Add replay instructions for Bronze to Silver.

## Interview Explanation

"The incident was not just an OOM. The OOM was a symptom of join skew caused by upstream data quality. The durable fix was key validation and skew detection, not only a larger cluster."
