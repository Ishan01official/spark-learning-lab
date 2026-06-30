# BOOK_MAP

Where each book chapter shows up in this repo. Use it backwards too: if you're studying a module, this tells you which book chapter to crack open.

## Tags

| Tag | Book |
|-----|------|
| **LS**  | *Learning Spark, 2nd Edition* — Damji et al. (O'Reilly, 2020) |
| **HPS** | *High Performance Spark, 2nd Edition* — Karau, Polak, Warren (O'Reilly, 2026) |
| **DAS** | *Data Algorithms with Spark* — Mahmoud Parsian (O'Reilly, 2022) |
| **Cert** | Databricks Certified Associate Developer for Apache Spark Using Python (exam guide) |

## Module → chapters

### 00_setup
- **LS** Ch.1 (Introduction), Ch.2 (Downloading Spark and Getting Started)
- **HPS** Ch.1 (Introduction to High Performance Spark)

### 01_fundamentals
- **LS** Ch.2 (Spark's Components), Ch.3 (Apache Spark's Structured APIs)
- **HPS** Ch.2 (How Spark Works), Ch.3 (DataFrames, Datasets, and Spark SQL)
- **DAS** Ch.1 (Introduction to Spark and PySpark), Ch.2 (Transformations in Action)
- **Cert** Domain 1 — Apache Spark Architecture (~17%)

### 02_pyspark_core
- **LS** Ch.3 (Structured APIs), Ch.4 (Spark SQL and DataFrames), Ch.5 (Spark SQL and DataFrames: Interacting with External Data Sources), Ch.6 (Spark SQL and Datasets)
- **HPS** Ch.3, Ch.4 (Joins), Ch.6 (Working with Key/Value Data)
- **DAS** Ch.4 (Reductions in Spark), Ch.5 (Partitioning Data), Ch.6 (Graph Algorithms)
- **Cert** Domain 2 — Spark DataFrame API Applications (~72%)

### 03_optimization
- **HPS** Ch.5 (Effective Transformations), Ch.7 (Going Beyond Scheduler), Ch.8 (Testing and Validation), Ch.9 (Tuning), Ch.10 (Spark Internals)
- **LS** Ch.7 (Optimizing and Tuning Spark Applications)
- **Cert** Domain 2 includes adaptive query execution, partitioning, caching

### 04_delta_lake
- **LS** Ch.9 (Building Reliable Data Lakes with Apache Spark)
- **HPS** Ch.11 (Beyond Spark SQL)

### 05_streaming
- **LS** Ch.8 (Structured Streaming)
- **HPS** Ch.12 (Spark Streaming)

### 06_real_projects
- **DAS** Ch.7–11 (Map-side join, reduce-side join, dimensions, dedup)
- **HPS** Ch.4 (Joins), Ch.6

### 07_interview_prep
- **Cert** — full domain coverage
  - Domain 1: Apache Spark Architecture and Components
  - Domain 2: Using Spark DataFrame API
  - Domain 3: Spark SQL functions
  - Domain 4: Datasets in Scala (PySpark variant ignores)

### 08_notes_from_books
- Condensed chapter notes for all three books.

### 09_latest_updates
- Spark release notes (3.4, 3.5, 4.0), Spark Connect, K8s deployments.

## Reverse lookup — when you open a book

### Reading *Learning Spark 2e*
- Ch.1–2 → `00_setup`
- Ch.3 → `01_fundamentals`, `02_pyspark_core`
- Ch.4–6 → `02_pyspark_core`
- Ch.7 → `03_optimization`
- Ch.8 → `05_streaming`
- Ch.9 → `04_delta_lake`

### Reading *High Performance Spark 2e*
- Ch.1–2 → `00_setup`, `01_fundamentals`
- Ch.3–4 → `02_pyspark_core`
- Ch.5–10 → `03_optimization`
- Ch.11 → `04_delta_lake`
- Ch.12 → `05_streaming`

### Reading *Data Algorithms with Spark*
- Ch.1–3 → `01_fundamentals`, `02_pyspark_core`
- Ch.4–6 → `02_pyspark_core`, `03_optimization`
- Ch.7–11 → `06_real_projects`
