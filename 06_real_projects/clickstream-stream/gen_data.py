"""
Producer: continuously writes new JSON event files into the landing dir.

Each file contains 50–100 events. Some events arrive late (out-of-order by 1–60s).
A small fraction (~3%) of event_ids are duplicates from earlier files.

Run:
    python gen_data.py
"""

import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

LANDING = Path("/tmp/clickstream_demo/landing")
EVENT_TYPES = ["page_view", "click", "scroll", "purchase", "search"]
N_USERS = 100

seen_ids = []  # track for occasional dup injection


def gen_event(now: datetime) -> dict:
    user_id = f"user-{random.randint(0, N_USERS-1):04d}"
    event_id = f"evt-{int(now.timestamp() * 1000)}-{random.randint(0, 99999)}"
    seen_ids.append(event_id)
    if len(seen_ids) > 10_000:
        seen_ids.pop(0)
    # Late events: 10% have event_time 1-60s in the past
    lag = random.randint(1, 60) if random.random() < 0.10 else 0
    event_time = now - timedelta(seconds=lag)
    return {
        "event_id":   event_id,
        "user_id":    user_id,
        "event_type": random.choice(EVENT_TYPES),
        "url":        f"/page/{random.randint(1, 50)}",
        "event_time": event_time.isoformat() + "Z",
    }


def main() -> None:
    LANDING.mkdir(parents=True, exist_ok=True)
    print(f"Producing events into {LANDING}/. Ctrl+C to stop.")
    file_no = 0
    try:
        while True:
            now = datetime.utcnow()
            n = random.randint(50, 100)
            events = [gen_event(now) for _ in range(n)]
            # Inject ~3% duplicates from recent history
            if seen_ids and random.random() < 0.3:
                dups = random.sample(seen_ids, k=min(3, len(seen_ids)))
                for d in dups:
                    events.append({
                        "event_id": d,
                        "user_id": f"user-{random.randint(0, N_USERS-1):04d}",
                        "event_type": random.choice(EVENT_TYPES),
                        "url": "/dup",
                        "event_time": now.isoformat() + "Z",
                    })
            path = LANDING / f"events_{int(time.time())}_{file_no}.json"
            with path.open("w") as f:
                for e in events:
                    f.write(json.dumps(e) + "\n")
            print(f"  wrote {path.name} ({len(events)} events)")
            file_no += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopped producer.")


if __name__ == "__main__":
    main()
