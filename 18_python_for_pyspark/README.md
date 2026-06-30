# 18_python_for_pyspark - Python Basics Needed For PySpark

You do not need to be a Python expert to learn PySpark, but you need enough Python to read examples and write clean jobs.

## Topics

- variables and types
- lists, dictionaries, tuples
- functions
- modules and imports
- virtual environments
- pathlib
- type hints
- simple tests
- logging basics
- avoiding Python UDFs unless needed

## PySpark-Specific Python Habits

- Use `pyspark.sql.functions as F`.
- Avoid `collect()` on large data.
- Avoid Python loops over DataFrame rows.
- Prefer Spark built-in functions to Python UDFs.
- Keep transformation functions small and testable.

## Learning Objective

Be able to read every example in `02_pyspark_core/examples/` without getting stuck on Python syntax.
