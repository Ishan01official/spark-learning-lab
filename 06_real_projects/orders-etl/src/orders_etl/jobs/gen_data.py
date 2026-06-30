"""
Generate sample order JSON files for the orders-etl pipeline.

Creates 3 days of orders, ~1000 per day, plus a customers reference.

Run:
    python -m orders_etl.jobs.gen_data
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from orders_etl.config import CONFIG


REGIONS = ["US", "EU", "APAC", "LATAM"]
SKUS    = [f"SKU-{c}" for c in "ABCDEFGHIJ"]
TIERS   = ["bronze", "silver", "gold", "platinum"]


def gen_customers(n: int = 200) -> list:
    return [
        {"customer_id": f"c-{i:04d}",
         "country":     random.choice(["US", "DE", "JP", "BR", "IN"]),
         "tier":        random.choice(TIERS)}
        for i in range(n)
    ]


def gen_order(order_id: int, day: datetime, customers: list) -> dict:
    cust = random.choice(customers)
    return {
        "order_id":   f"o-{day.strftime('%Y%m%d')}-{order_id:05d}",
        "customer_id": cust["customer_id"],
        "sku":         random.choice(SKUS),
        "amount":      round(random.uniform(5, 500), 2),
        "region":      random.choice(REGIONS) if random.random() > 0.05 else None,
        "order_time":  (day + timedelta(seconds=random.randint(0, 86399))).isoformat() + "Z",
    }


def gen_bad_order(order_id: int) -> dict:
    """Some intentionally malformed records — for the dead-letter exercise."""
    return {"order_id": f"o-bad-{order_id}", "amount": -1, "junk": "missing fields"}


def main() -> None:
    random.seed(42)
    landing = Path(CONFIG.landing_dir)
    landing.mkdir(parents=True, exist_ok=True)

    customers = gen_customers()
    # Write the customers reference file once
    with (landing.parent / "customers.json").open("w") as f:
        for c in customers:
            f.write(json.dumps(c) + "\n")

    for day_offset in range(3):
        day = datetime(2024, 9, 15) + timedelta(days=day_offset)
        path = landing / f"orders_{day.strftime('%Y%m%d')}.json"
        with path.open("w") as f:
            for i in range(1000):
                f.write(json.dumps(gen_order(i, day, customers)) + "\n")
            # Sprinkle in a handful of bad records
            for i in range(5):
                f.write(json.dumps(gen_bad_order(i)) + "\n")
        print(f"  wrote {path} ({1005} records, 5 bad)")

    print(f"\nDone. {len(customers)} customers + 3 days of orders generated under {CONFIG.landing_dir}/")


if __name__ == "__main__":
    main()
