/// tx-inspector — debug consumer for the "transactions" topic.
///
/// Validates each message against the shared Transaction schema,
/// tracks basic statistics, and pretty-prints anomalous transactions.
/// Run this while developing to verify that the simulator produces
/// well-formed data before handing off to Student B's Flink pipeline.

use anyhow::Result;
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use shared::{Transaction, TOPIC_TRANSACTIONS};
use std::collections::HashMap;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "tx-inspector", about = "Debug consumer — validates transaction JSON")]
struct Args {
    #[arg(long, default_value = "localhost:9092")]
    broker: String,

    /// Consumer group ID (change to re-read from the beginning)
    #[arg(long, default_value = "tx-inspector-dev")]
    group: String,

    /// Print every valid transaction, not just anomalous ones
    #[arg(long)]
    verbose: bool,

    /// Only show messages for a specific card ID
    #[arg(long)]
    filter_card: Option<String>,
}

// ── Running statistics ────────────────────────────────────────────────────────

#[derive(Default)]
struct Stats {
    received:   u64,
    valid:      u64,
    invalid:    u64,
    anomalous:  u64,
    /// Per-anomaly-kind counts.
    by_kind:    HashMap<String, u64>,
    /// Amount min/max seen so far.
    amount_min: f64,
    amount_max: f64,
    amount_sum: f64,
}

impl Stats {
    fn new() -> Self {
        Self {
            amount_min: f64::MAX,
            amount_max: f64::MIN,
            ..Default::default()
        }
    }

    fn record(&mut self, tx: &Transaction) {
        self.valid += 1;
        self.amount_min = self.amount_min.min(tx.amount_pln);
        self.amount_max = self.amount_max.max(tx.amount_pln);
        self.amount_sum += tx.amount_pln;
        if let Some(kind) = &tx.injected_anomaly {
            self.anomalous += 1;
            *self.by_kind.entry(format!("{kind:?}")).or_insert(0) += 1;
        }
    }

    fn print_summary(&self) {
        let avg = if self.valid > 0 { self.amount_sum / self.valid as f64 } else { 0.0 };
        info!(
            received  = self.received,
            valid     = self.valid,
            invalid   = self.invalid,
            anomalous = self.anomalous,
            "── Statistics ──"
        );
        info!(
            min = format!("{:.2}", self.amount_min),
            max = format!("{:.2}", self.amount_max),
            avg = format!("{:.2}", avg),
            "Amount (PLN)"
        );
        for (kind, count) in &self.by_kind {
            info!(kind, count, "Anomaly count");
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("tx-inspector starting, group={}", args.group);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers",     &args.broker)
        .set("group.id",              &args.group)
        .set("auto.offset.reset",     "earliest")
        .set("enable.auto.commit",    "true")
        .set("auto.commit.interval.ms", "1000")
        .create()?;

    consumer.subscribe(&[TOPIC_TRANSACTIONS])?;

    let mut stats = Stats::new();
    let mut last_print = std::time::Instant::now();

    loop {
        let msg = consumer.recv().await?;
        stats.received += 1;

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s))  => s,
            Some(Err(e)) => {
                error!("UTF-8 decode error: {e}");
                stats.invalid += 1;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
            None => {
                warn!("Empty payload at offset {}", msg.offset());
                stats.invalid += 1;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        // ── Schema validation ─────────────────────────────────────────────────
        let tx: Transaction = match serde_json::from_str(payload) {
            Ok(t) => t,
            Err(e) => {
                error!(
                    offset = msg.offset(),
                    error  = %e,
                    raw    = %payload,
                    "JSON parse / schema validation failed"
                );
                stats.invalid += 1;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        // ── Sanity checks ─────────────────────────────────────────────────────
        let mut issues = Vec::<&str>::new();
        if tx.amount_pln <= 0.0          { issues.push("amount ≤ 0"); }
        if tx.remaining_limit_pln < 0.0  { issues.push("limit < 0"); }
        if tx.location.lat.abs() > 90.0  { issues.push("lat out of range"); }
        if tx.location.lon.abs() > 180.0 { issues.push("lon out of range"); }
        if tx.card_id.is_empty()         { issues.push("empty card_id"); }

        if !issues.is_empty() {
            warn!(
                card_id = %tx.card_id,
                issues  = ?issues,
                "Transaction failed sanity checks"
            );
        }

        // ── Filtering ─────────────────────────────────────────────────────────
        if let Some(ref filter) = args.filter_card {
            if &tx.card_id != filter {
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        }

        stats.record(&tx);

        // ── Output ────────────────────────────────────────────────────────────
        let should_print = args.verbose || tx.injected_anomaly.is_some() || !issues.is_empty();
        if should_print {
            // Pretty-print using serde_json so Flink developers can inspect the schema.
            let pretty = serde_json::to_string_pretty(&tx)?;
            let tag = if let Some(ref kind) = tx.injected_anomaly {
                format!("[ANOMALY:{kind}]")
            } else if !issues.is_empty() {
                "[INVALID]".to_string()
            } else {
                "[OK]".to_string()
            };
            println!("\n{tag} offset={}\n{pretty}", msg.offset());
        }

        // Print stats every 5 seconds.
        if last_print.elapsed().as_secs() >= 5 {
            stats.print_summary();
            last_print = std::time::Instant::now();
        }

        consumer.commit_message(&msg, CommitMode::Async)?;
    }
}