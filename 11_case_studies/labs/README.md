# Runnable Case Study Labs

These labs turn production incidents into local experiments.

Run them from the repo root with the virtual environment activated:

```bash
python 11_case_studies/labs/01_skewed_join_lab.py
python 11_case_studies/labs/02_small_files_lab.py
```

## Lab 1 - Skewed Join

What it teaches:

- A hot key can dominate a join stage.
- Spark UI exposes skew through long-tail tasks and uneven shuffle.
- Salting can spread one hot key across multiple partitions.

What to inspect:

- Stages tab.
- Task duration distribution.
- Shuffle read per task.
- SQL physical plan.

## Lab 2 - Small Files

What it teaches:

- Over-partitioned writes create too many files.
- File count is a production metric.
- Write strategy is part of table design.

What to inspect:

- Output file count under `tmp/case_studies/small_files/`.
- Job/stage count in Spark UI.
- Difference between uncontrolled and intentional output partitioning.
