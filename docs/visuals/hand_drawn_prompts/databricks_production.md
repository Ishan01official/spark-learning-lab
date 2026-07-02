# Databricks Production

## Purpose

Show the path from notebook exploration to governed scheduled production workflow.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on white paper. Use rough
black pen, simple stations, arrows, small characters, and sparse red/blue handwritten annotations.

Concept: Databricks production workflow. Draw a loose assembly line from left to right: notebook
sketch, repo code, pull request gate, job cluster, Unity Catalog guardrail, scheduled workflow, and
monitoring bell. A tiny serious black helper character moves a small Spark job card through each
station. Add one red warning sign near manual notebook runs and one blue note near governed tables.

The core idea: exploration becomes production only after version control, job configuration,
permissions, scheduling, and monitoring.
```

## Layout description

Horizontal assembly line with six stations. Keep each station simple: notebook, repo, PR gate, job
cluster, governance, monitor.

## Labels to include

- `notebook`
- `repo code`
- `PR`
- `job cluster`
- `Unity Catalog`
- `monitoring`
- `no manual prod` in red

## Visual style

White paper, rough black pen, one small operator character carrying a job card, blue for governance
note, red for manual-run warning, no vendor UI screenshot.

## What to avoid

- Real Databricks screenshots
- Overloaded platform diagram
- Marketing-style cloud icons
- Too many governance details

## Suggested caption

A notebook becomes production only when it passes through code review, job configuration,
permissions, scheduling, and monitoring.
