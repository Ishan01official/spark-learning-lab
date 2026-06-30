# Troubleshooting

Use this page when an example fails before you understand the Spark concept being taught.

## Quick Checks

```bash
python --version
java -version
pip show pyspark
python scripts/validate_repo.py
```

## `JAVA_HOME is not set`

Install a JDK, then set `JAVA_HOME` to the JDK directory.

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

## `Py4JJavaError`

This is a wrapper around a JVM-side Spark error. Scroll to the first `Caused by:` line in the stack trace. That is usually the real issue.

Common causes:

- Java version mismatch.
- Bad file path.
- Schema mismatch.
- Executor memory pressure.
- Unsupported Delta or Spark version combination.

## `AnalysisException`

Usually caused by missing columns, wrong table names, wrong paths, or schema assumptions.

Check:

```python
df.printSchema()
df.show(5, truncate=False)
```

## Spark UI Is Missing

The job may have finished. Spark UI only exists while the driver is alive.

For learning, temporarily add this at the end of a script:

```python
input("Press Enter to stop Spark...")
```

Then open:

```text
http://localhost:4040
```

## Port `4040` Is Busy

Spark will try the next available port. Look for a log line like:

```text
SparkUI: Bound SparkUI to 0.0.0.0, and started at http://...
```

## Local Disk Fills Up

Remove generated Spark artifacts:

```bash
make clean
```

Also check for output folders created by examples:

```bash
find . -maxdepth 3 -type d \( -name output -o -name tmp -o -name spark-warehouse \)
```

## Delta Example Fails

Confirm compatible versions:

```bash
pip show pyspark delta-spark
```

This repo currently pins PySpark 3.5.1 and Delta Spark 3.2.0.
