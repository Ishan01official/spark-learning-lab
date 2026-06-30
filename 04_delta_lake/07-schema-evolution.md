# 07 — Schema evolution and enforcement

## Why this matters

Production tables outlive their schemas. You add a `country` column three months in. You realize a typo in a column name. You want to drop a deprecated field. Delta gives you tools for all of this, plus a hard guarantee: writes that would *silently* corrupt the schema get rejected.

## Two opposite defaults

| Mode | Default? | Behavior |
|---|---|---|
| **Schema enforcement** | Yes | A write with a column not in the table fails. Mismatched types fail. |
| **Schema evolution** | No (opt-in) | New columns in the incoming DataFrame are added to the table. |

Enforcement is the safety net. Evolution is the escape hatch.

## Enforcement examples

```python
# Existing table:  (id BIGINT, name STRING)

# This fails — `email` is not in the table:
new_df.write.format("delta").mode("append").save("/tables/users")
# AnalysisException: A schema mismatch detected when writing to the Delta table.

# This also fails — `id` was BIGINT, but new_df has it as STRING:
# AnalysisException: Failed to merge ... incompatible types
```

Enforced checks:
- Column names match (case-sensitive depending on `spark.sql.caseSensitive`).
- Column types are compatible (BIGINT → BIGINT, etc.). Some widenings are allowed (INT → BIGINT, etc.); narrowings are not.
- `NOT NULL` constraints respected.
- `CHECK` constraints respected on every row.

## Schema evolution — adding columns

```python
new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/tables/users")
```

With `mergeSchema=true`, columns present in `new_df` but not in the table are added to the table schema. Existing rows get `NULL` for the new column.

Restrictions:
- You can add columns. You can't drop or rename them this way.
- You can't change a column's type (except some widenings).
- New columns must have a name not already used (case-sensitivity rules apply).

For MERGE, the equivalent option is set on the writer:

```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# Now MERGE will accept new columns from the source
```

## Schema overwrite

When you really want to replace the schema entirely (e.g. you're rewriting the table layout):

```python
new_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/tables/users")
```

This replaces both data *and* schema in one commit. Use with care — readers running an old query against the new schema may break.

## Manual schema operations (SQL DDL)

```sql
-- Add a column (nullable, default NULL)
ALTER TABLE users ADD COLUMN signup_source STRING;

-- Add a column at a specific position
ALTER TABLE users ADD COLUMN region STRING AFTER country;

-- Drop a column (requires column mapping mode)
ALTER TABLE users DROP COLUMN obsolete_flag;

-- Rename a column (requires column mapping mode)
ALTER TABLE users RENAME COLUMN signup_source TO acquisition_channel;

-- Change nullability
ALTER TABLE users ALTER COLUMN email DROP NOT NULL;

-- Change column comment
ALTER TABLE users ALTER COLUMN email COMMENT 'verified email address';
```

## Column mapping mode

By default, Delta uses **column-name-based** mapping: the file column name *is* the logical column name. That means RENAME and DROP would rewrite every file.

Column mapping mode separates the logical name from the physical (file) name:

```sql
ALTER TABLE users SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);
```

After this: RENAME and DROP only update metadata. Files don't move. Old readers (those that don't understand column mapping) can no longer read the table — hence the protocol bump.

**One-way street**: once you enable column mapping, you can't turn it off. Plan accordingly.

[*Delta Definitive Guide* Ch.10 §"Column Mapping"]

## Type widening (Delta 3.2+)

```sql
ALTER TABLE orders ALTER COLUMN order_id TYPE BIGINT;  -- was INT
```

Supported widenings without rewriting data:
- BYTE → SHORT → INT → BIGINT
- FLOAT → DOUBLE
- DECIMAL(p, s) → DECIMAL(p', s') where p' - s' ≥ p - s (more precision, same scale)
- DATE → TIMESTAMP (only in some configurations)

Requires writer version 7 + reader version 3.

## What happens to old data when you evolve

| Operation | What happens to existing files |
|---|---|
| Add column | Files untouched; reads return NULL for the new column |
| Drop column (with mapping) | Files untouched; column not returned to queries |
| Rename column (with mapping) | Files untouched; logical → physical map updated |
| Widen type (e.g. INT → BIGINT) | Files untouched; reads cast on read |
| Change nullability | Just metadata; future writes enforce |
| `overwriteSchema=true` | All data rewritten — this is a full replace |

## Patterns

### Pattern: graceful onboarding of new fields

Source schema gets a new field. You want to start capturing it without coordinating with downstream:

```python
# Enable autoMerge for one job
new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/tables/events")

# Downstream readers see the new column as NULL for historical rows
```

### Pattern: deprecation

```sql
-- Step 1: announce + leave column. Stop populating it.
ALTER TABLE users ALTER COLUMN legacy_id COMMENT 'DEPRECATED 2024-10. Will be dropped 2025-01';

-- Step 2 (months later): drop it.
ALTER TABLE users DROP COLUMN legacy_id;  -- requires column mapping
```

### Pattern: rename in two steps if column mapping isn't enabled

```sql
-- Step 1: add the new column, dual-write for a release.
ALTER TABLE users ADD COLUMN email_address STRING;
-- (App code now writes both `email` and `email_address`)

-- Step 2: backfill.
UPDATE users SET email_address = email WHERE email_address IS NULL;

-- Step 3: switch readers to email_address, drop legacy field.
ALTER TABLE users DROP COLUMN email;
```

This is the safest path — no protocol changes, no risk to old readers.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `AnalysisException: schema mismatch` | Source DF has extra columns, or type mismatch | Use `mergeSchema=true`, or align schemas |
| `Cannot drop column: column mapping not enabled` | Tried DROP/RENAME without column mapping | Enable column mapping (one-way) |
| Old reader broken after enabling column mapping | Protocol bump required min reader v2 | Upgrade reader library |
| Adding NOT NULL column fails on existing data | Existing rows would be NULL | Add column nullable + backfill + then ALTER NOT NULL |
| `overwriteSchema=true` wiped a downstream view | Schema changed mid-stream | Communicate breaking changes; version views |
| Type cast failures after widening | ANSI mode strict | Test reads in ANSI; adjust |

## References

- *Delta Lake: The Definitive Guide* — Ch.10 "Schema Evolution"
- Delta docs: https://docs.delta.io/latest/delta-batch.html#-schema-validation
- [LS Ch.9 §"Schema Enforcement"]
- 📺 [Schema Evolution in Delta Lake — Databricks](https://www.youtube.com/results?search_query=delta+lake+schema+evolution+databricks)
