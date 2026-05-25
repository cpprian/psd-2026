#!/usr/bin/env python3
"""
mock_alert_producer.py — Student A's dev tool.

Produces fake Alert JSON to the "alerts" topic so that alarm-watcher
can be developed and tested independently from Student B's Flink app.

Usage:
    pip install kafka-python
    python mock_alert_producer.py --broker localhost:9092 --rate 2
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

ANOMALY_KINDS = [
    "large_amount",
    "impossible_travel",
    "high_frequency",
    "new_geography",
    "limit_exhaustion",
    "structuring",
]

DESCRIPTIONS = {
    "large_amount":      "Amount {:.0f} PLN is {:.1f}σ above 30-day mean",
    "impossible_travel": "Card used {:.0f} km from previous location within {:.0f} min",
    "high_frequency":    "{} transactions in the last 60 s (baseline: {})",
    "new_geography":     "First transaction in {} (home region: Poland)",
    "limit_exhaustion":  "Remaining limit {:.0f} PLN after spending {:.0f} PLN",
    "structuring":       "{} transactions just below {:.0f} PLN threshold",
}

REGIONS = ["Germany", "USA", "China", "Japan", "Brazil", "Nigeria"]


def random_alert(card_id: str) -> dict:
    kind = random.choice(ANOMALY_KINDS)
    severity = round(random.betavariate(2, 1.5), 3)  # skewed toward higher severities
    amount = round(random.uniform(50, 15000), 2)
    limit  = round(random.uniform(0, 5000), 2)

    if kind == "large_amount":
        desc = DESCRIPTIONS[kind].format(amount, random.uniform(3, 12))
    elif kind == "impossible_travel":
        desc = DESCRIPTIONS[kind].format(random.randint(500, 15000), random.randint(1, 20))
    elif kind == "high_frequency":
        desc = DESCRIPTIONS[kind].format(random.randint(10, 60), random.randint(1, 5))
    elif kind == "new_geography":
        desc = DESCRIPTIONS[kind].format(random.choice(REGIONS))
    elif kind == "limit_exhaustion":
        desc = DESCRIPTIONS[kind].format(limit, amount)
    else:  # structuring
        desc = DESCRIPTIONS[kind].format(random.randint(3, 12), random.choice([500, 1000, 5000]))

    # Construct a minimal Transaction snapshot (must match shared::Transaction schema).
    tx = {
        "transaction_id":      str(uuid.uuid4()),
        "card_id":             card_id,
        "user_id":             f"USER-{card_id[-3:]}",
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "location":            {
            "lat": round(random.uniform(49.0, 54.9), 6),
            "lon": round(random.uniform(14.1, 24.1), 6),
        },
        "amount_pln":          amount,
        "remaining_limit_pln": limit,
        "merchant":            random.choice(["Biedronka", "Allegro", "Bolt", "Netflix"]),
        "injected_anomaly":    kind,
    }

    return {
        "alert_id":      str(uuid.uuid4()),
        "transaction_id": tx["transaction_id"],
        "card_id":        card_id,
        "user_id":        tx["user_id"],
        "timestamp":      tx["timestamp"],
        "anomaly_kind":   kind,
        "description":    desc,
        "severity":       severity,
        "transaction":    tx,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--broker", default="localhost:9092")
    p.add_argument("--rate",   type=float, default=1.0, help="Alerts per second")
    p.add_argument("--cards",  type=int,   default=20,  help="Number of simulated cards")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    cards = [f"CARD-{i:06}" for i in range(args.cards)]
    print(f"Mock alert producer started — {args.rate:.1f} alerts/s → topic 'alerts'")

    interval = 1.0 / args.rate
    sent = 0
    try:
        while True:
            card_id = random.choice(cards)
            alert   = random_alert(card_id)
            producer.send("alerts", key=card_id, value=alert)
            sent += 1
            if sent % 20 == 0:
                print(f"Sent {sent} mock alerts")
            time.sleep(interval)
    except KeyboardInterrupt:
        producer.flush()
        print(f"\nStopped. Total: {sent} alerts sent.")


if __name__ == "__main__":
    main()