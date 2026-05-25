/// tx-simulator — M2-4 complete: all 6 anomaly types.
///
/// Run:
///   cargo run --bin tx-simulator
///   cargo run --bin tx-simulator -- --anomaly-rate 0.05
///   cargo run --bin tx-simulator -- --tps 200 --cards 10000

mod fleet;
mod anomaly;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use fleet::{build_fleet, round2, CardState, Region};
use rand::prelude::*;
use rand::rngs::SmallRng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use shared::{Transaction, TOPIC_TRANSACTIONS};
use std::time::{Duration, Instant};
use uuid::Uuid;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "tx-simulator", about = "Send payment transactions (all 6 anomaly types) to Kafka")]
struct Args {
    #[arg(long, default_value = "localhost:9092")]
    broker: String,

    #[arg(long, default_value_t = 10_000)]
    cards: usize,

    /// Target messages per second (normal cadence; burst anomalies may briefly exceed this).
    #[arg(long, default_value_t = 100)]
    tps: u64,

    /// Probability [0.0–1.0] that any given tick triggers an anomaly.
    /// Default 0.01 = 1%. Use 0.05 during development to see anomalies faster.
    #[arg(long, default_value_t = 0.01)]
    anomaly_rate: f64,
}

// ── Merchant names ────────────────────────────────────────────────────────────

const MERCHANTS_POLAND:        &[&str] = &["Biedronka","Lidl","Żabka","Orlen","BP","InPost","Allegro","PKP Intercity","Empik"];
const MERCHANTS_EUROPE:        &[&str] = &["Carrefour","REWE","Tesco","Shell","Aldi","H&M","Zara","DHL","Lufthansa"];
const MERCHANTS_NORTH_AMERICA: &[&str] = &["Walmart","Target","Chevron","Starbucks","Amazon","Uber","CVS","Delta Airlines"];
const MERCHANTS_EAST_ASIA:     &[&str] = &["FamilyMart","7-Eleven","Lawson","UnionPay","Grab","JD.com","ANA","Rakuten"];

fn merchants_for(region: Region) -> &'static [&'static str] {
    match region {
        Region::Poland        => MERCHANTS_POLAND,
        Region::WesternEurope => MERCHANTS_EUROPE,
        Region::NorthAmerica  => MERCHANTS_NORTH_AMERICA,
        Region::EastAsia      => MERCHANTS_EAST_ASIA,
    }
}

// ── Normal transaction builder ────────────────────────────────────────────────

fn make_normal_tx(card: &mut CardState, rng: &mut SmallRng) -> Transaction {
    let raw_amount = card.typical_amount * rng.gen_range(0.5_f64..=1.5);
    let amount     = round2(raw_amount.max(1.0).min(card.limit.max(1.0)));

    let new_limit = if rng.gen_bool(0.005) {
        round2(rng.gen_range(1_000.0_f64..=20_000.0))
    } else {
        round2((card.limit - amount).max(0.0))
    };

    let location = card.home_region.sample_location(rng);
    let merchant = merchants_for(card.home_region).choose(rng).unwrap().to_string();

    card.limit         = new_limit;
    card.last_location = location.clone();
    card.visited_regions.insert(card.home_region);

    Transaction {
        transaction_id:      Uuid::new_v4(),
        card_id:             card.card_id.clone(),
        user_id:             card.user_id.clone(),
        timestamp:           Utc::now(),
        location,
        amount_pln:          amount,
        remaining_limit_pln: new_limit,
        merchant,
        injected_anomaly:    None,
    }
}

// ── Rate limiter ──────────────────────────────────────────────────────────────

struct RateLimiter { target_tps: u64, tokens: f64, last_refill: Instant }

impl RateLimiter {
    fn new(tps: u64) -> Self {
        Self { target_tps: tps, tokens: 1.0, last_refill: Instant::now() }
    }

    fn wait(&mut self) {
        let tps = self.target_tps as f64;
        loop {
            let now = Instant::now();
            self.tokens += now.duration_since(self.last_refill).as_secs_f64() * tps;
            self.last_refill = now;
            if self.tokens > tps { self.tokens = tps; }
            if self.tokens >= 1.0 { self.tokens -= 1.0; return; }
            std::thread::sleep(Duration::from_secs_f64((1.0 - self.tokens) / tps));
        }
    }
}

// ── Progress tracker ──────────────────────────────────────────────────────────

struct Progress {
    sent:      u64,
    anomalous: u64,
    errors:    u64,
    start:     Instant,
}

impl Progress {
    fn new() -> Self {
        Self { sent: 0, anomalous: 0, errors: 0, start: Instant::now() }
    }

    fn on_send_ok(&mut self, was_anomalous: bool) {
        self.sent += 1;
        if was_anomalous { self.anomalous += 1; }
        if self.sent % 1_000 == 0 { self.print(); }
    }

    fn on_send_err(&mut self, e: &rdkafka::error::KafkaError) {
        self.errors += 1;
        eprintln!("[error] Kafka send failed: {e}");
    }

    fn print(&self) {
        let elapsed  = self.start.elapsed().as_secs_f64();
        let anom_pct = if self.sent > 0 { self.anomalous as f64 / self.sent as f64 * 100.0 } else { 0.0 };
        println!(
            "[{:>8.1}s]  sent={:>8}  anomalous={:>6} ({:>5.2}%)  errors={:>4}  tps={:.1}",
            elapsed, self.sent, self.anomalous, anom_pct, self.errors,
            self.sent as f64 / elapsed,
        );
    }
}

// ── Kafka helpers ─────────────────────────────────────────────────────────────

fn make_producer(broker: &str) -> Result<BaseProducer> {
    ClientConfig::new()
        .set("bootstrap.servers",           broker)
        .set("message.timeout.ms",          "5000")
        .set("linger.ms",                   "20")
        .set("batch.num.messages",          "1000")
        .set("compression.type",            "lz4")
        .set("queue.buffering.max.messages", "100000")
        .create()
        .context("Failed to create Kafka producer — is docker compose running?")
}

fn send_tx(
    producer:  &BaseProducer,
    card_id:   &str,
    tx:        &Transaction,
    progress:  &mut Progress,
    anomalous: bool,
) -> Result<()> {
    let json = serde_json::to_string(tx).context("serialize transaction")?;
    let record = BaseRecord::to(TOPIC_TRANSACTIONS)
        .key(card_id)
        .payload(json.as_str());
    match producer.send(record) {
        Ok(())         => progress.on_send_ok(anomalous),
        Err((e, _msg)) => progress.on_send_err(&e),
    }
    producer.poll(Duration::ZERO);
    Ok(())
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let args = Args::parse();

    // 1. Build fleet.
    println!("Building fleet of {} cards...", args.cards);
    let mut rng   = SmallRng::from_entropy();
    let mut fleet = build_fleet(args.cards, &mut rng);
    println!(
        "Fleet ready. TPS={}  anomaly_rate={:.1}%  broker={}",
        args.tps, args.anomaly_rate * 100.0, args.broker
    );
    println!("Anomaly types active: LargeAmount, ImpossibleTravel, HighFrequency,");
    println!("                      NewGeography, LimitExhaustion, Structuring");
    println!("Press Ctrl-C to stop.\n");

    // 2. Connect to Kafka.
    let producer = make_producer(&args.broker)?;

    // 3. Send loop.
    let mut limiter  = RateLimiter::new(args.tps);
    let mut progress = Progress::new();

    loop {
        limiter.wait();

        let idx  = rng.gen_range(0..fleet.len());
        let card = &mut fleet[idx];

        // maybe_inject returns Vec<Transaction>:
        //   - length 1 for all types except HighFrequency
        //   - length 5–15 for HighFrequency bursts
        // All transactions in the vec share the same card_id so they land
        // on the same Kafka partition and are seen in order by Flink.
        let (txs, was_anomalous) = anomaly::maybe_inject(
            card,
            make_normal_tx,
            &mut rng,
            args.anomaly_rate,
        );

        // For burst anomalies: first tx counted as anomalous, rest as normal
        // so the TPS counter isn't inflated (the burst is one anomaly event).
        for (i, tx) in txs.iter().enumerate() {
            send_tx(&producer, &card.card_id.clone(), tx, &mut progress, was_anomalous && i == 0)?;
        }
    }
}