# Card Anomaly Detection

Real-time payment card fraud detection using Kafka, Flink, and MongoDB.

## Architecture

```
tx-simulator
      │
      ▼  topic: transactions
  Apache Kafka
      │
      ├──► tx-inspector            — validates messages, prints anomalies
      │
      └──► Flink detector          — detects anomalies, raises alerts
                  │
                  ▼  topic: alerts
              Kafka
                  │
                  ├──► alarm-watcher          — live TUI dashboard
                  │
                  └──► MongoDB                — persistent alert storage
```

## Components

| Component | Language | Description |
|---|---|---|
| `tx-simulator` | Rust | Produces transactions at configurable TPS with 6 anomaly types |
| `tx-inspector` | Rust | Debug consumer — validates JSON, prints anomalies and stats |
| `alarm-watcher` | Rust | Live terminal dashboard for Flink alerts |
| `mock_tx_producer.py` | Python | Fake transactions for testing Flink without the Rust simulator |
| `mock_alert_producer.py` | Python | Fake alerts for testing alarm-watcher without Flink |

## Anomaly Types

| Type | Detection signal |
|---|---|
| `large_amount` | Amount > 3σ above the card's 30-transaction mean |
| `impossible_travel` | Required travel speed > 900 km/h between two transactions |
| `high_frequency` | > 5× the card's baseline transaction rate in 60 seconds |
| `new_geography` | Transaction in a region the card has never used |
| `limit_exhaustion` | Single transaction drains ≥ 95% of remaining limit |
| `structuring` | Amount just below 500 / 1 000 / 5 000 PLN threshold |

## Quick Start

Requires: Docker Desktop, Rust (`rustup`), Python 3.

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Build
cargo build

# 3. Run simulator (terminal 1)
cargo run --bin tx-simulator -- --anomaly-rate 0.05 --tps 50

# 4. Run inspector (terminal 2)
cargo run --bin tx-inspector

# 5. Test alarm-watcher with mock alerts (terminal 3)
pip install kafka-python
python mock_alert_producer.py --rate 2

# 6. Run dashboard (terminal 4)
cargo run --bin alarm-watcher
```

See [`docs/instruction.md`](docs/INSTRUCTION.md) for a full step-by-step setup guide.

## Kafka Topics

| Topic | Producer | Consumers |
|---|---|---|
| `transactions` | `tx-simulator` | `tx-inspector`, Flink detector |
| `alerts` | Flink detector | `alarm-watcher`, MongoDB writer |

Broker: `localhost:9092`

## JSON Contract

### Transaction

```json
{
  "transaction_id":      "uuid",
  "card_id":             "CARD-000042",
  "user_id":             "USER-000030",
  "timestamp":           "2026-05-25T22:15:01.123Z",
  "location":            { "lat": 52.24, "lon": 21.02 },
  "amount_pln":          149.99,
  "remaining_limit_pln": 3850.01,
  "merchant":            "Biedronka",
  "injected_anomaly":    "large_amount"
}
```

> `injected_anomaly` is absent on normal transactions. Flink must **not** use it for detection — it exists for accuracy evaluation only.

### Alert

```json
{
  "alert_id":       "uuid",
  "transaction_id": "uuid",
  "card_id":        "CARD-000042",
  "user_id":        "USER-000030",
  "timestamp":      "2026-05-25T22:15:02.001Z",
  "anomaly_kind":   "large_amount",
  "description":    "Amount 8.3σ above the 30-transaction mean.",
  "severity":       0.91,
  "transaction":    { }
}
```

## Services

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Flink Web UI | http://localhost:8081 |
| Mongo Express | http://localhost:8082 |
| Kafka broker | localhost:9092 |
| MongoDB | localhost:27017 |
