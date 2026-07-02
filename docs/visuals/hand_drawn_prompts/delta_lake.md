# Delta Lake

## Purpose

Explain Delta Lake as data files governed by a transaction log.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on white paper. Use rough
black pen lines, simple folders, file cards, arrows, and a few red/blue handwritten annotations.

Concept: Delta Lake architecture. Draw a lake made of simple Parquet file cards. Beside the lake,
draw a small ledger notebook labeled _delta_log. A serious tiny black helper character stamps the
ledger each time a new file card is added or removed. Draw a reader character looking only at the
ledger first, then choosing the active file cards from the lake.

The core idea: Delta tables are Parquet files plus a transaction log that defines the current table
snapshot.
```

## Layout description

Parquet file lake on the left/bottom, transaction ledger on the right, commit stamp between them,
reader follows the ledger to active files.

## Labels to include

- `Parquet files`
- `_delta_log`
- `commit`
- `snapshot`
- `ACID`
- `time travel` in blue

## Visual style

White paper, rough black line art, ledger metaphor, blue for reader/time-travel note, red only for
commit stamp or warning mark.

## What to avoid

- Database cylinder icons
- Too much ACID theory text
- Vendor logos
- Complex file listings

## Suggested caption

Delta Lake turns a folder of Parquet files into a table by keeping a ledger of exactly which files
belong to each version.
