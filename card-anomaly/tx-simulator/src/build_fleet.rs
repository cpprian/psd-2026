/// build-fleet — M2-1: generate 10 000 cards and print a summary.
///
/// Run:
///   cargo run --bin build-fleet
///   cargo run --bin build-fleet -- --cards 500
///   cargo run --bin build-fleet -- --show 20
///   cargo run --bin build-fleet -- --seed 42

mod fleet;

use clap::Parser;
use fleet::{build_fleet, CardState};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::collections::HashMap;

#[derive(Parser)]
#[command(name = "build-fleet", about = "Generate the card fleet and print a summary")]
struct Args {
    #[arg(long, default_value_t = 10_000)]
    cards: usize,

    #[arg(long, default_value_t = 10)]
    show: usize,

    #[arg(long)]
    seed: Option<u64>,
}

// ── Stats helpers ─────────────────────────────────────────────────────────────

fn print_summary(fleet: &[CardState], elapsed_ms: f64) {
    let n = fleet.len();
    let mut per_user: HashMap<&str, usize> = HashMap::new();
    for c in fleet { *per_user.entry(&c.user_id).or_insert(0) += 1; }

    let mut amounts: Vec<f64> = fleet.iter().map(|c| c.typical_amount).collect();
    amounts.sort_by(f64::total_cmp);
    let a_mean = amounts.iter().sum::<f64>() / n as f64;
    let a_p50  = amounts[n / 2];
    let a_p90  = amounts[(n as f64 * 0.90) as usize];

    let l_min  = fleet.iter().map(|c| c.limit).fold(f64::MAX, f64::min);
    let l_max  = fleet.iter().map(|c| c.limit).fold(f64::MIN, f64::max);
    let l_mean = fleet.iter().map(|c| c.limit).sum::<f64>() / n as f64;

    let mut by_region: Vec<(String, usize)> = {
        let mut m: HashMap<String, usize> = HashMap::new();
        for c in fleet { *m.entry(c.home_region.name().to_string()).or_insert(0) += 1; }
        m.into_iter().collect()
    };
    by_region.sort_by(|a, b| a.0.cmp(&b.0));

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║              Card fleet — summary                    ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Cards generated :  {:>10}                      ║", n);
    println!("║  Unique users    :  {:>10}                      ║", per_user.len());
    println!("║  Users 1 card    :  {:>10}                      ║", per_user.values().filter(|&&v| v==1).count());
    println!("║  Users 2 cards   :  {:>10}                      ║", per_user.values().filter(|&&v| v==2).count());
    println!("║  Build time      :  {:>10.2} ms                 ║", elapsed_ms);
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Typical amount (PLN)                                ║");
    println!("║    min  {:>8.2}   max  {:>8.2}   mean {:>8.2}  ║",
        amounts[0], amounts[n-1], a_mean);
    println!("║    p50  {:>8.2}   p90  {:>8.2}                  ║", a_p50, a_p90);
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Spending limit (PLN)                                ║");
    println!("║    min  {:>8.2}   max  {:>8.2}   mean {:>8.2}  ║", l_min, l_max, l_mean);
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Home region distribution                            ║");
    for (region, count) in &by_region {
        println!("║    {:<20}  {:>5}  ({:>5.1}%)              ║",
            region, count, *count as f64 / n as f64 * 100.0);
    }
    println!("╚══════════════════════════════════════════════════════╝");
}

fn assert_fleet(fleet: &[CardState], expected_n: usize) {
    assert_eq!(fleet.len(), expected_n, "Fleet length mismatch");
    let mut seen_ids = std::collections::HashSet::new();
    for c in fleet {
        assert!(!c.card_id.is_empty());
        assert!(!c.user_id.is_empty());
        assert!(c.typical_amount > 0.0, "typical_amount must be positive");
        assert!(c.limit >= 1_000.0 && c.limit <= 20_000.0, "limit out of range");
        assert!(c.last_location.lat.abs() <= 90.0);
        assert!(c.last_location.lon.abs() <= 180.0);
        assert!(seen_ids.insert(c.card_id.as_str()), "duplicate card_id");
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    let mut rng = match args.seed {
        Some(s) => SmallRng::seed_from_u64(s),
        None    => SmallRng::from_entropy(),
    };

    let t0    = std::time::Instant::now();
    let fleet = build_fleet(args.cards, &mut rng);
    let ms    = t0.elapsed().as_secs_f64() * 1000.0;

    print_summary(&fleet, ms);

    if args.show > 0 {
        let show = args.show.min(fleet.len());
        println!();
        println!("Sample cards (first {show}):");
        println!("{:<14} {:<14} {:>12}  {:>10}  {:<18}  home lat/lon",
            "card_id", "user_id", "typical_amt", "limit", "region");
        println!("{}", "─".repeat(90));
        for c in fleet.iter().take(show) {
            println!("{:<14} {:<14} {:>11.2}  {:>10.2}  {:<18}  {:.4}/{:.4}",
                c.card_id, c.user_id, c.typical_amount, c.limit,
                c.home_region.name(),
                c.last_location.lat, c.last_location.lon);
        }
    }

    assert_fleet(&fleet, args.cards);
    println!();
    println!("All assertions passed. Fleet is ready.");
    println!("Next step: cargo run --bin tx-simulator");
}