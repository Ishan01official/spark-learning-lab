# Spark UI Debugging

## Purpose

Turn Spark UI troubleshooting into a memorable detective-board image.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on white paper. Use rough
black pen, simple sticky notes, arrows, small charts, and sparse red/blue handwritten annotations.

Concept: Spark UI debugging. Draw a detective board with four pinned cards: Jobs, Stages, SQL, and
Executors. A tiny serious black helper character connects red string from a symptom card reading
slow job to a stage card with one very long task bar. Add a small spill bucket near Executors and a
plan scroll near SQL. Keep it playful but technically clear.

The core idea: diagnose with evidence from Spark UI tabs before changing configs.
```

## Layout description

Detective board in center, symptom card on left, UI tab cards across the board, red string to the
straggler evidence, blue notes for metrics.

## Labels to include

- `Jobs`
- `Stages`
- `SQL`
- `Executors`
- `straggler`
- `spill` in red
- `evidence first` in blue

## Visual style

Hand-drawn detective board, black pen, red string only for the suspicious path, blue for helpful
metric notes, no real UI screenshot.

## What to avoid

- Copying the actual Spark UI layout
- Listing every tab and metric
- Dark detective theme
- Making the character decorative only

## Suggested caption

The Spark UI is a detective board: find the slow stage, identify the straggler, then choose the
fix.
