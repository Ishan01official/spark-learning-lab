# Streaming Checkpoint Issues

## Symptoms

- Streaming job cannot restart.
- Duplicate output after restart.
- State store grows without bound.
- Query says checkpoint is incompatible.

## Root Causes

- Deleted checkpoint.
- Changed query shape incompatibly.
- Reused checkpoint path for another query.
- Missing watermark.
- Changed stateful operator.
- Sink is not idempotent.

## What To Check First

1. Checkpoint path.
2. Query id and source offsets.
3. Recent code changes.
4. State operator metrics.
5. Output sink behavior.

## Fix Options

- Restore checkpoint if available.
- Use a new checkpoint only when duplicate/replay risk is understood.
- Make `foreachBatch` writes idempotent.
- Add watermark for bounded state.
- Backfill from Bronze when replay is required.

## Prevention

- Stable checkpoint path per query.
- Do not share checkpoints.
- Version streaming query changes.
- Keep raw Bronze data for replay.

## Interview Explanation

"A checkpoint is part of the correctness contract, not a temp folder. Deleting it can cause duplicates or data loss depending on the source and sink."
