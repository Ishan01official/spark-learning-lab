# clickstream-stream exercises

---

## 1. Switch the source from files to Kafka

Replace the file source in `stream_bronze.py` with a Kafka source. You'll need:
- A local Kafka (or Redpanda) instance.
- A producer that publishes to a topic (modify `gen_data.py`).
- Configure `startingOffsets`, `maxOffsetsPerTrigger`.

Compare the two: which is easier to operate? Easier to scale?

---

## 2. Restart safety

While the pipeline is running:
1. `Ctrl+C` `run_all.py`.
2. Restart it.
3. Verify no events are processed twice.

How do you confirm? (Hint: count events in silver before and after; should match exactly the count of unique event_ids in bronze.)

---

## 3. Bad data injection

Modify `gen_data.py` to occasionally write malformed JSON (random bytes, truncated lines). Then:
- Verify bronze keeps absorbing them (it should — it stores as TEXT).
- Verify silver filters them out (they should not appear in silver).
- Add a dead-letter table for the bad rows in silver.
- Add a DQ alert if dead-letter rate exceeds 1%.

---

## 4. Tune the session window

The current session is "5-minute gap, 15-minute watermark". This means:
- A session emits 5+15 = 20 minutes after the user's last event.
- Too long for "real-time analytics".

Tighten to "2-minute gap, 5-minute watermark". Run for an hour and observe:
- Are sessions shorter and more frequent?
- Any sessions cut off mid-activity?

---

## 5. Add a stream-static join

Add a `user_dim` table (static) with `user_id, country, tier`. Enrich silver with these columns.

Two ways:
- Stream-static join in silver — what happens if `user_dim` updates while the stream is running?
- foreachBatch reading user_dim each batch — what's the cost?

---

## 6. Production runbook

Write a runbook covering:
- What metrics to monitor for each of the three streams.
- Alerts: at what level of lag, dropped-by-watermark, batch duration, do you page?
- How to safely upgrade `event_schema` if a new field is added upstream.
- How to safely change the session window definition (it changes state — beware).
- Backfill: how would you reprocess a day of bronze when a silver bug is fixed?
