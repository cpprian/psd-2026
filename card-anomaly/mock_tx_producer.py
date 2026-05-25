#!/usr/bin/env python3
"""
mock_tx_producer.py — Student B's dev tool.

Produces fake Transaction JSON to the "transactions" topic so that
the Flink anomaly detector can be developed without waiting for
Student A's Rust simulator to be ready.

Produces all six anomaly types so that Flink rules can be exercised
in isolation.

Usage:
    pip install kafka-python
    python mock_tx_producer.py --broker localhost:9092 --tps 20
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

MERCHANTS = ["Biedronka", "Lidl", "Żabka", "Orlen", "McDonalds", "Allegro", "Netflix", "Bolt"]


def poland_coords():
    return {"lat": round(random.uniform(49.0, 54.9), 6),
            "lon": round(random.uniform(14.1, 24.1), 6)}


def foreign_coords():
    return {"lat": round(random.uniform(35.0, 60.0), 6),
            "lon": round(random.uniform(-10.0, 140.0), 6)}


class CardState:
    def __init__(self, card_id, user_id):
        self.card_id         = card_id
        self.user_id         = user_id
        self.typical_amount  = random.uniform(20, 600)
        self.limit           = random.uniform(2000, 15000)
        self.last_location   = poland_coords()

    def normal_tx(self):
        amount = round(self.typical_amount * random.uniform(0.6, 1.4), 2)
        amount = min(amount, self.limit)
        self.limit -= amount
        if random.random() < 0.005:
            self.limit = random.uniform(2000, 15000)
        return self._tx(amount, poland_coords())

    def anomaly_tx(self, kind):
        if kind == "large_amount":
            amount = round(self.typical_amount * random.uniform(5, 15), 2)
            loc = poland_coords()
        elif kind == "impossible_travel":
            amount = round(self.typical_amount * random.uniform(0.8, 1.2), 2)
            loc = foreign_coords()
        elif kind == "high_frequency":
            amount = round(self.typical_amount * 0.9, 2)
            loc = poland_coords()
        elif kind == "new_geography":
            amount = round(self.typical_amount * random.uniform(0.8, 1.5), 2)
            loc = foreign_coords()
        elif kind == "limit_exhaustion":
            amount = round(self.limit * 0.95, 2)
            loc = poland_coords()
        elif kind == "structuring":
            threshold = random.choice([500, 1000, 5000])
            amount = round(threshold - random.uniform(1, 50), 2)
            loc = poland_coords()
        else:
            return self.normal_tx()

        amount = max(0.01, min(amount, self.limit))
        self.limit -= amount
        return self._tx(amount, loc, kind)

    def _tx(self, amount, loc, injected=None):
        tx = {
            "transaction_id":      str(uuid.uuid4()),
            "card_id":             self.card_id,
            "user_id":             self.user_id,
            "timestamp":           datetime.now(timezone.utc).isoformat(),
            "location":            loc,
            "amount_pln":          round(amount, 2),
            "remaining_limit_pln": round(max(self.limit, 0), 2),
            "merchant":            random.choice(MERCHANTS),
        }
        if injected:
            tx["injected_anomaly"] = injected
        return tx


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--broker",       default="localhost:9092")
    p.add_argument("--tps",          type=int,   default=20)
    p.add_argument("--cards",        type=int,   default=200)
    p.add_argument("--anomaly-rate", type=float, default=0.03)
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    n_users = max(1, args.cards // 2)
    cards = [
        CardState(f"CARD-{i:06}", f"USER-{i % n_users:06}")
        for i in range(args.cards)
    ]

    KINDS = ["large_amount","impossible_travel","high_frequency",
             "new_geography","limit_exhaustion","structuring"]

    interval = 1.0 / args.tps
    sent = 0

    print(f"Mock TX producer: {args.tps} TPS, {args.cards} cards, "
          f"{args.anomaly_rate*100:.1f}% anomaly rate → topic 'transactions'")

    try:
        while True:
            card = random.choice(cards)
            if random.random() < args.anomaly_rate:
                kind = random.choice(KINDS)
                tx = card.anomaly_tx(kind)
            else:
                tx = card.normal_tx()

            producer.send("transactions", key=card.card_id, value=tx)
            sent += 1
            if sent % 500 == 0:
                print(f"Sent {sent} transactions")
            time.sleep(interval)
    except KeyboardInterrupt:
        producer.flush()
        print(f"\nStopped. Total: {sent} sent.")


if __name__ == "__main__":
    main()