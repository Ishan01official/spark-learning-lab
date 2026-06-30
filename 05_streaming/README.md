# 05 — Structured Streaming

Streaming in Spark is just DataFrame operations applied to an unbounded table. Same API as batch, same Catalyst plans, plus a few streaming-specific concerns: triggers, watermarks, checkpointing, output modes, and stateful operations.

## What you should be able to do after this

- Read from and write to streaming sources/sinks (files, Kafka, Delta, foreachBatch).
- Choose the right output mode (append / update / complete) and trigger.
- Reason about exactly-once semantics and what guarantees each sink provides.
- Use watermarks to bound state for late-arriving event-time data.
- Implement stream-stream and stream-static joins.
- Build a Kafka → Delta bronze ingestion pipeline.

## Notes

1. [The Structured Streaming model](01-the-model.md)
2. [Sources and sinks](02-sources-and-sinks.md)
3. [Triggers, output modes, and checkpointing](03-triggers-modes-checkpoints.md)
4. [Event time and watermarks](04-event-time-and-watermarks.md)
5. [Stateful aggregations](05-stateful-aggregations.md)
6. [Stream-stream and stream-static joins](06-streaming-joins.md)
7. [Kafka deep dive](07-kafka.md)
8. [foreachBatch and arbitrary sinks](08-foreachbatch.md)
9. [Streaming exactly-once and failure handling](09-exactly-once-and-failures.md)

## Book references

- [LS Ch.8] — "Structured Streaming"
- [HPS Ch.10] — "Streaming with Spark"
- [DAS Ch.11] — "Streaming algorithms"
- Apache Spark Streaming Programming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
