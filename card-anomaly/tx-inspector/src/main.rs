use anyhow::{Context, Result};
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use shared::{AnomalyKind, Transaction, TOPIC_TRANSACTIONS};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "tx-inspector", about = "Validate and print transactions from Kafka")]
struct Args {
    #[arg(long, default_value = "localhost:9092")]
    broker: String,

    #[arg(long, default_value = "tx-inspector-dev")]
    group: String,

    #[arg(long, default_value = "earliest")]
    offset: String,

    #[arg(long)]
    verbose: bool,

    #[arg(long)]
    filter_card: Option<String>,
}

const RED:    &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const GREEN:  &str = "\x1b[32m";
const CYAN:   &str = "\x1b[36m";
const BOLD:   &str = "\x1b[1m";
const DIM:    &str = "\x1b[2m";
const RESET:  &str = "\x1b[0m";

fn colour_for_anomaly(kind: &AnomalyKind) -> &'static str {
    match kind {
        AnomalyKind::LargeAmount     => RED,
        AnomalyKind::ImpossibleTravel => RED,
        AnomalyKind::HighFrequency   => YELLOW,
        AnomalyKind::NewGeography    => YELLOW,
        AnomalyKind::LimitExhaustion => RED,
        AnomalyKind::Structuring     => CYAN,
    }
}

fn sanity_check(tx: &Transaction) -> Vec<String> {
    let mut issues = Vec::new();

    if tx.amount_pln <= 0.0 {
        issues.push(format!("amount_pln = {:.2} is not positive", tx.amount_pln));
    }
    if tx.remaining_limit_pln < 0.0 {
        issues.push(format!("remaining_limit_pln = {:.2} is negative", tx.remaining_limit_pln));
    }
    if tx.location.lat < -90.0 || tx.location.lat > 90.0 {
        issues.push(format!("lat = {:.4} is outside [-90, 90]", tx.location.lat));
    }
    if tx.location.lon < -180.0 || tx.location.lon > 180.0 {
        issues.push(format!("lon = {:.4} is outside [-180, 180]", tx.location.lon));
    }
    if tx.card_id.is_empty() {
        issues.push("card_id is empty".to_string());
    }
    if tx.user_id.is_empty() {
        issues.push("user_id is empty".to_string());
    }
    if tx.merchant.is_empty() {
        issues.push("merchant is empty".to_string());
    }

    issues
}

struct Stats {
    received:    u64,
    valid:       u64,
    invalid:     u64,
    bad_fields:  u64,
    anomalous:   u64,
    by_kind:     HashMap<String, u64>,
    amount_min:  f64,
    amount_max:  f64,
    amount_sum:  f64,
    start:       Instant,
    last_print:  Instant,
}

impl Stats {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            received: 0, valid: 0, invalid: 0, bad_fields: 0, anomalous: 0,
            by_kind: HashMap::new(),
            amount_min: f64::MAX, amount_max: f64::MIN, amount_sum: 0.0,
            start: now, last_print: now,
        }
    }

    fn record_valid(&mut self, tx: &Transaction, has_issues: bool) {
        self.valid += 1;
        if has_issues { self.bad_fields += 1; }

        self.amount_min  = self.amount_min.min(tx.amount_pln);
        self.amount_max  = self.amount_max.max(tx.amount_pln);
        self.amount_sum += tx.amount_pln;

        if let Some(kind) = &tx.injected_anomaly {
            self.anomalous += 1;
            *self.by_kind.entry(format!("{kind}")).or_insert(0) += 1;
        }
    }

    fn should_print(&self) -> bool {
        self.last_print.elapsed().as_secs() >= 5
    }

    fn print_summary(&mut self) {
        let elapsed  = self.start.elapsed().as_secs_f64();
        let msg_rate = self.received as f64 / elapsed;
        let avg      = if self.valid > 0 { self.amount_sum / self.valid as f64 } else { 0.0 };

        // Box-drawing summary table
        println!("\n{BOLD}┌─────────────────────────────────────────────┐{RESET}");
        println!("{BOLD}│              tx-inspector stats               │{RESET}");
        println!("{BOLD}├─────────────────────────────────────────────┤{RESET}");
        println!("│  elapsed       {DIM}{:>8.1} s{RESET}                   │", elapsed);
        println!("│  received      {BOLD}{:>8}{RESET}   ({:.1} msg/s)      │", self.received, msg_rate);
        println!("│  valid         {GREEN}{:>8}{RESET}                       │", self.valid);
        println!("│  invalid JSON  {RED}{:>8}{RESET}                       │", self.invalid);
        println!("│  bad fields    {YELLOW}{:>8}{RESET}                       │", self.bad_fields);
        println!("│  anomalous     {CYAN}{:>8}{RESET}                       │", self.anomalous);
        println!("{BOLD}├─────────────────────────────────────────────┤{RESET}");
        println!("│  amount (PLN)  min {:<8.2}  max {:<8.2}  │",
            if self.amount_min == f64::MAX { 0.0 } else { self.amount_min },
            if self.amount_max == f64::MIN { 0.0 } else { self.amount_max });
        println!("│                avg {:<30.2}  │", avg);

        if !self.by_kind.is_empty() {
            println!("{BOLD}├─────────────────────────────────────────────┤{RESET}");
            let mut kinds: Vec<(&String, &u64)> = self.by_kind.iter().collect();
            kinds.sort_by(|a, b| b.1.cmp(a.1));
            for (kind, count) in kinds {
                println!("│  {CYAN}{:<22}{RESET} {:>5} anomalies      │", kind, count);
            }
        }

        println!("{BOLD}└─────────────────────────────────────────────┘{RESET}\n");
        self.last_print = Instant::now();
    }
}

// ── Message printer ───────────────────────────────────────────────────────────

fn print_transaction(
    tx:      &Transaction,
    offset:  i64,
    issues:  &[String],
    verbose: bool,
) {
    let is_anomalous = tx.injected_anomaly.is_some();
    let is_invalid   = !issues.is_empty();

    if !verbose && !is_anomalous && !is_invalid {
        return;
    }

    let (colour, tag) = if is_anomalous {
        let kind = tx.injected_anomaly.as_ref().unwrap();
        (colour_for_anomaly(kind), format!("ANOMALY:{kind}"))
    } else if is_invalid {
        (YELLOW, "INVALID".to_string())
    } else {
        (GREEN, "OK".to_string())
    };

    println!(
        "\n{BOLD}{colour}[{tag}]{RESET}  {DIM}offset={offset}  partition skipped{RESET}",
    );

    // Core fields — always shown.
    println!(
        "  {DIM}card{RESET}  {BOLD}{}{RESET}  {DIM}user{RESET} {}  {DIM}merchant{RESET} {}",
        tx.card_id, tx.user_id, tx.merchant
    );
    println!(
        "  {DIM}amount{RESET}  {BOLD}{colour}{:.2} PLN{RESET}  {DIM}limit remaining{RESET}  {:.2} PLN",
        tx.amount_pln, tx.remaining_limit_pln
    );
    println!(
        "  {DIM}location{RESET}  lat {:.4}  lon {:.4}  {DIM}time{RESET}  {}",
        tx.location.lat, tx.location.lon, tx.timestamp
    );

    // Sanity-check failures — shown in yellow.
    if is_invalid {
        for issue in issues {
            println!("  {YELLOW}⚠  {issue}{RESET}");
        }
    }

    if verbose {
        if let Ok(pretty) = serde_json::to_string_pretty(tx) {
            println!("{DIM}{pretty}{RESET}");
        }
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("{BOLD}tx-inspector{RESET} connecting to {}  group={}  offset={}",
        args.broker, args.group, args.offset);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers",       &args.broker)
        .set("group.id",                &args.group)
        .set("auto.offset.reset",       &args.offset)
        .set("enable.auto.commit",      "true")
        .set("auto.commit.interval.ms", "1000")
        .create()
        .context("Failed to create Kafka consumer — is docker compose running?")?;

    consumer
        .subscribe(&[TOPIC_TRANSACTIONS])
        .context("Failed to subscribe to topic")?;

    println!("Subscribed to {BOLD}{TOPIC_TRANSACTIONS}{RESET}. Waiting for messages…");
    if !args.verbose {
        println!("{DIM}(only anomalous and invalid messages are printed — use --verbose for all){RESET}");
    }
    if let Some(ref f) = args.filter_card {
        println!("{DIM}filter: card_id contains \"{f}\"{RESET}");
    }
    println!();

    let mut stats = Stats::new();

    loop {
        let msg = consumer.recv().await
            .context("Kafka recv error")?;

        stats.received += 1;

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s))  => s,
            Some(Err(_)) => {
                println!("{RED}[INVALID]{RESET} offset={}  payload is not valid UTF-8",
                    msg.offset());
                stats.invalid += 1;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
            None => {
                println!("{YELLOW}[WARN]{RESET} offset={}  empty payload, skipping",
                    msg.offset());
                stats.invalid += 1;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        let tx: Transaction = match serde_json::from_str(payload) {
            Ok(t)  => t,
            Err(e) => {
                println!("{RED}[INVALID]{RESET} offset={}  JSON parse failed: {e}", msg.offset());
                println!("{DIM}  raw: {}{RESET}", &payload[..payload.len().min(200)]);
                stats.invalid += 1;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        if let Some(ref filter) = args.filter_card {
            if !tx.card_id.contains(filter.as_str()) {
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        }

        let issues = sanity_check(&tx);

        stats.record_valid(&tx, !issues.is_empty());

        print_transaction(&tx, msg.offset(), &issues, args.verbose);

        if stats.should_print() {
            stats.print_summary();
        }

        consumer.commit_message(&msg, CommitMode::Async)?;
    }
}