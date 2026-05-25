use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use shared::{GpsCoords, Transaction, TOPIC_TRANSACTIONS};
use std::time::Duration;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "send-one", about = "Send one test transaction to Kafka")]
struct Args {
    /// Kafka broker address
    #[arg(long, default_value = "localhost:9092")]
    broker: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let tx = Transaction {
        transaction_id:      Uuid::new_v4(),
        card_id:             "CARD-000001".to_string(),
        user_id:             "USER-000001".to_string(),
        timestamp:           Utc::now(),
        location:            GpsCoords::new(52.237049, 21.017532), // Warsaw, Poland
        amount_pln:          149.99,
        remaining_limit_pln: 3850.01,
        merchant:            "Biedronka".to_string(),
        injected_anomaly:    None,
    };

    let json = serde_json::to_string_pretty(&tx)
        .context("Failed to serialize transaction to JSON")?;

    println!("─── Transaction to send ───────────────────────────────────────");
    println!("{json}");
    println!("───────────────────────────────────────────────────────────────");
    println!();

    println!("Connecting to Kafka at {} ...", args.broker);

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .set("message.timeout.ms", "5000")
        .create()
        .context(
            "Could not create Kafka producer. \
             Is docker compose running? Try: docker compose ps"
        )?;

    let key = tx.card_id.clone();

    producer
        .send(
            BaseRecord::to(TOPIC_TRANSACTIONS)
                .payload(json.as_bytes())
                .key(key.as_bytes()),
        )
        .map_err(|(e, _)| anyhow::anyhow!("Queue error: {e}"))?;

    println!("Message queued. Flushing (waiting for delivery confirmation)...");

    producer
        .flush(Duration::from_secs(10))
        .context("Flush timed out — Kafka did not confirm delivery within 10 s")?;

    println!();
    println!("✓  Message delivered successfully!");
    println!();
    println!("Next steps:");
    println!("  1. Open Kafka UI at http://localhost:8080");
    println!("  2. Click Topics → transactions");
    println!("  3. Click the Messages tab");
    println!("  4. You should see one message with key \"{}\"", tx.card_id);
    println!("  5. Click it to expand and compare the JSON with what was printed above");
    println!();
    println!("transaction_id = {}", tx.transaction_id);

    Ok(())
}