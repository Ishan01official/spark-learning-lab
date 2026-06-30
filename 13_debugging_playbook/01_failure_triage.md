# Failure Triage

## Symptoms

- Job failed.
- Notebook command failed.
- Workflow task failed.
- Spark application exited with non-zero status.

## Error Examples

- `AnalysisException`
- `Py4JJavaError`
- `OutOfMemoryError`
- `FileNotFoundException`
- `PermissionDenied`
- `ConcurrentAppendException`

## Root Causes

- Bad path or missing file.
- Schema mismatch.
- Permission issue.
- Driver or executor memory pressure.
- Bad dependency.
- Delta concurrent write.
- Logic bug in Python code.

## What To Check First

1. First error, not last error.
2. Driver logs.
3. Executor logs.
4. Spark UI failed stage.
5. Input path and table permissions.
6. Recent code or source data changes.

## Spark UI Areas

- Jobs tab for failed job.
- Stages tab for failed stage.
- SQL tab for query plan.
- Executors tab for lost executors or memory pressure.

## Fix Options

- Correct path, schema, or permission.
- Add explicit schema.
- Reduce selected columns.
- Fix join condition.
- Make writes idempotent.
- Pin compatible library versions.

## Interview Explanation

"I start from the first meaningful error and map it to a Spark component: driver, executor, source, shuffle, sink, or permissions. Then I validate the suspected root cause with Spark UI or logs before changing configs."
