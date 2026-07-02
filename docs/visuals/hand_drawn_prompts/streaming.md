# Streaming

## Purpose

Make Structured Streaming's micro-batch loop easy to remember.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on white paper. Use rough
black pen, simple conveyor belt shapes, boxes, arrows, and sparse red/blue handwritten annotations.

Concept: Structured Streaming. Draw an endless conveyor belt entering from the left with event
cards. A small clock chops the belt into micro-batches. In the center, executor workers process one
batch at a time. Below them, draw a small state drawer. On the right, draw a sink box. Above the
engine, draw a checkpoint notebook that records offsets and batch completion. A tiny serious black
helper character writes in the checkpoint notebook.

The core idea: Spark repeatedly processes new data as micro-batches and uses checkpoints to resume.
```

## Layout description

Source conveyor left, micro-batch clock near center, processing workers center, state drawer below,
sink right, checkpoint notebook above.

## Labels to include

- `source`
- `micro-batch`
- `state`
- `sink`
- `checkpoint`
- `resume here` in blue

## Visual style

Simple conveyor sketch, black pen, blue checkpoint note, tiny red mark only for failure/retry, lots
of blank space.

## What to avoid

- Real Kafka UI
- Complex stream-stream join details
- Too many event cards
- Flashy real-time dashboard style

## Suggested caption

Structured Streaming is a reliable loop: read new records, process one micro-batch, write output,
checkpoint progress, repeat.
