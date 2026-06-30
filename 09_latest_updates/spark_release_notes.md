# Spark Release Notes Tracker

This page is a lightweight tracker for Spark changes that matter to a PySpark data engineer. It is not a replacement for official release notes; use it to decide what to test in this repo.

## How To Review A New Spark Release

For each Spark release, check:

1. PySpark API changes.
2. SQL behavior changes.
3. Structured Streaming fixes and compatibility notes.
4. Spark Connect changes.
5. Kubernetes and deployment changes.
6. Performance improvements around Catalyst, AQE, shuffle, Parquet, and Arrow.
7. Deprecated configs or changed defaults.

## Suggested Update Format

```text
## Spark x.y.z

### Why It Matters
- ...

### Features To Test
- ...

### Possible Breakages
- ...

### Repo Follow-Up
- [ ] Add or update example
- [ ] Add note to relevant module
- [ ] Add interview/prep question if important
```

## Spark 3.5.x Watchlist

- Spark Connect maturity and PySpark parity.
- Structured Streaming operational improvements.
- SQL function additions and behavior changes.
- Python dependency compatibility, especially pandas and pyarrow.
- AQE, shuffle, and file-source performance fixes.

## Spark 4.x Watchlist

- Removed deprecated APIs.
- Compatibility with Java versions used in local labs.
- PySpark migration issues.
- Spark SQL ANSI behavior and changed defaults.
- Connector ecosystem readiness.

## Official Sources To Check

- Apache Spark downloads and release notes.
- Apache Spark migration guides.
- PySpark API documentation.
- Databricks Runtime release notes when using Databricks.
- Delta Lake release notes for storage-layer compatibility.
