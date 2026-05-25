# Card Anomaly Detection — Setup Guide

---

## What you get

| Component | What it does |
|---|---|
| `tx-simulator` | Generates 10 000 fake payment-card transactions per second and sends them to Kafka. Injects 6 types of anomalies at a configurable rate. |
| `tx-inspector` | Reads from Kafka, validates every transaction, and prints anomalies to the terminal with colour-coded output and a stats summary every 5 seconds. |
| `alarm-watcher` | A live terminal dashboard that reads alerts from the `alerts` Kafka topic and displays them in a table with bar charts. Designed to show alerts produced by your Flink detector. |
| `mock_alert_producer.py` | A Python script that sends fake alerts to Kafka so you can test `alarm-watcher` without Flink running. |
| `mock_tx_producer.py` | A Python script that sends fake transactions to Kafka so you can test your Flink detector without the Rust simulator running. |

---

## Step 1 — Start the infrastructure

This starts Kafka, Flink, MongoDB, and their web UIs using Docker.

```bash
docker compose up -d
```

Wait about 20 seconds, then check everything is running:

```bash
docker compose ps
```

You should see all services with status **Up**. If something shows **Exit**, wait another 10 seconds and run the command again.

### Web UIs (open in your browser)

| URL | What it shows |
|---|---|
| http://localhost:8080 | Kafka UI — browse topics and messages |
| http://localhost:8081 | Flink Web UI — submit and monitor your Flink job |
| http://localhost:8082 | Mongo Express — browse the MongoDB database |

### Create the Kafka topics

The topics are usually created automatically by `kafka-init`. If not (you will see errors mentioning "Unknown topic"), create them manually:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic transactions \
  --partitions 6 --replication-factor 1

docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic alerts \
  --partitions 3 --replication-factor 1
```

Verify both exist:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

Expected output:
```
alerts
transactions
```

---

## Step 2 — Build the Rust binaries

This compiles all components. The **first build takes 3–5 minutes** because it downloads and compiles dependencies. Subsequent builds are a few seconds.

```bash
cargo build
```

If you see errors about missing system libraries, go back to the Prerequisites section and install the build tools for your OS.

---

## Step 3 — Run the transaction simulator

Open a **new terminal**, go to the project folder, and run:

```bash
cargo run --bin tx-simulator -- --anomaly-rate 0.05 --tps 30
```

**Keep this terminal open.** The simulator runs continuously until you press `Ctrl-C`.

#### Options

| Flag | Default | Description |
|---|---|---|
| `--tps` | 100 | Messages per second |
| `--anomaly-rate` | 0.01 | Fraction of transactions that are anomalous (0.05 = 5%) |
| `--cards` | 10000 | Number of simulated cards |
| `--broker` | localhost:9092 | Kafka broker address |

Use `--anomaly-rate 0.05` during development so anomalies appear frequently. Switch to `0.01` for the final demo.

---

## Step 5 — Run the transaction inspector

Open another **new terminal** and run:

```bash
cargo run --bin tx-inspector -- --broker localhost:9092
```

This reads from the `transactions` topic and prints anomalous and invalid messages:

Stats print automatically every 5 seconds. Normal transactions are not printed unless you add `--verbose`.

#### Options

| Flag | Description |
|---|---|
| `--verbose` | Print every transaction, not just anomalous ones |
| `--filter-card CARD-000042` | Only show messages for this card |
| `--offset latest` | Only show messages that arrive after startup |
| `--group fresh` | Re-read all messages from the beginning |

---

## Step 6 — View messages in Kafka UI

Open **http://localhost:8080** in your browser.

1. Click **Topics** in the left menu
2. Click **transactions**
3. Click the **Messages** tab

You will see every transaction as it arrives. Click any message to expand the full JSON. To see only anomalous messages, click **Filters** and type `injected_anomaly` in the value field.

The `injected_anomaly` field is set by the simulator when a transaction is intentionally anomalous. **Your Flink detector must NOT use this field** — it is for testing accuracy only (comparing what was injected vs what Flink detected).

---

## Step 7 — Test your Flink detector without the Rust simulator

If you want to test your Flink job independently without waiting for the Rust simulator, use the Python mock:

```bash
# Create a virtual environment (only once)
python3 -m venv .venv
source .venv/bin/activate        # on Windows: .venv\Scripts\activate
pip install kafka-python

# Send fake transactions
python mock_tx_producer.py --tps 20 --anomaly-rate 0.05
```

This sends the same JSON schema as the Rust simulator so your Flink code will work with both.

---

## Step 8 — Run the alert dashboard

Once your Flink detector is running and producing alerts to the `alerts` topic, open the live dashboard:

```bash
cargo run --bin alarm-watcher
```

The dashboard shows:
- A live table of alerts (newest first) with time, card ID, anomaly type, severity, and description
- A bar chart of alert counts per anomaly type
- A severity histogram

**Keybindings:**

| Key | Action |
|---|---|
| `↑` / `↓` or `j` / `k` | Scroll the alert table |
| `f` | Type a card ID filter, then press Enter |
| `c` | Clear the active filter |
| `q` | Quit |

#### Test the dashboard without Flink

You can test `alarm-watcher` immediately without Flink using the mock alert producer:

```bash
# In one terminal
source .venv/bin/activate
python mock_alert_producer.py --rate 2

# In another terminal
cargo run --bin alarm-watcher
```

---

## Kafka connection details

Use these to connect your Flink job to Kafka:

| Setting | Value |
|---|---|
| Bootstrap server | `localhost:9092` |
| Transactions topic | `transactions` |
| Alerts topic | `alerts` |
| Offset reset | `earliest` |

MongoDB connection string for saving alerts:

```
mongodb://admin:secret@localhost:27017/anomaly_detection
```

Collection name: `alerts`

---

## JSON schemas

### Transaction (topic: `transactions`)

This is what the simulator produces. Your Flink job reads this.

```json
{
  "transaction_id":      "uuid-v4",
  "card_id":             "CARD-000042",
  "user_id":             "USER-000030",
  "timestamp":           "2026-05-25T22:15:01.123Z",
  "location":            { "lat": 52.237049, "lon": 21.017532 },
  "amount_pln":          149.99,
  "remaining_limit_pln": 3850.01,
  "merchant":            "Biedronka",
  "injected_anomaly":    "large_amount"
}
```

> `injected_anomaly` is **optional** — it is absent on normal transactions. Do not use it for detection.

### Alert (topic: `alerts`)

This is what your Flink job must produce. The `alarm-watcher` reads this.

```json
{
  "alert_id":       "uuid-v4",
  "transaction_id": "uuid-v4",
  "card_id":        "CARD-000042",
  "user_id":        "USER-000030",
  "timestamp":      "2026-05-25T22:15:02.001Z",
  "anomaly_kind":   "large_amount",
  "description":    "Amount 8.3 standard deviations above the 30-transaction mean.",
  "severity":       0.91,
  "transaction":    { ...full Transaction object... }
}
```

`anomaly_kind` must be one of: `large_amount`, `impossible_travel`, `high_frequency`, `new_geography`, `limit_exhaustion`, `structuring`.

`severity` must be a number between `0.0` (low) and `1.0` (critical).

---

## Anomaly types reference

| Type | What the simulator injects | How to detect in Flink |
|---|---|---|
| `large_amount` | Amount 5–15× the card's typical amount | Z-score > 3σ on a sliding window of last 30 transactions per card |
| `impossible_travel` | Location on a different continent, minutes after the previous transaction | Haversine distance / time elapsed > 900 km/h |
| `high_frequency` | 5–15 transactions for the same card within a few seconds | Count per card in a 60-second sliding window > 5× the card's baseline |
| `new_geography` | Transaction in a region the card has never used | Card's location history does not include this region |
| `limit_exhaustion` | One transaction drains 95–99% of the remaining limit | `amount / (amount + remaining_limit_pln) > 0.95` |
| `structuring` | Amount just below 500, 1 000, or 5 000 PLN | Amount in `[threshold - 50, threshold)` for any of the three thresholds |

---

## Stopping everything

```bash
# Stop the Rust programs
Ctrl-C in each terminal

# Stop Docker (keeps data)
docker compose down

# Stop Docker and delete all data (clean reset)
docker compose down -v
```

---

## Troubleshooting

**`cargo build` fails with "No such file or directory" for a C header**
→ Install the system build tools listed in Prerequisites for your OS.

**`Unknown topic or partition` when running tx-inspector or tx-simulator**
→ The Kafka topics were not created. Follow the "Create the Kafka topics" step above.

**`Connection refused` on port 9092**
→ Docker is not running, or the Kafka container has not started yet. Run `docker compose ps` and wait for all services to show `Up`.

**The first `cargo build` is very slow**
→ Normal. It is downloading and compiling all dependencies (~200 crates). Subsequent builds are much faster.

**`alarm-watcher` shows no alerts**
→ Either Flink is not running, or it is not writing to the `alerts` topic. Use `mock_alert_producer.py` to test the dashboard independently.

**On macOS: `xcrun: error: invalid active developer path`**
→ Run `xcode-select --install` and try again.