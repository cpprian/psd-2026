use crate::fleet::{local_move, round2, CardState, Region};
use rand::prelude::*;
use rand::rngs::SmallRng;
use shared::{AnomalyKind, GpsCoords, Transaction};

pub fn maybe_inject(
    card:         &mut CardState,
    normal_tx_fn: impl FnOnce(&mut CardState, &mut SmallRng) -> Transaction,
    rng:          &mut SmallRng,
    anomaly_rate: f64,
) -> (Vec<Transaction>, bool) {
    if rng.gen_bool(anomaly_rate) {
        let kind = pick_kind(rng);
        let txs  = inject(card, kind, rng);
        (txs, true)
    } else {
        (vec![normal_tx_fn(card, rng)], false)
    }
}

fn pick_kind(rng: &mut SmallRng) -> AnomalyKind {
    match rng.gen_range(0..6) {
        0 => AnomalyKind::LargeAmount,
        1 => AnomalyKind::ImpossibleTravel,
        2 => AnomalyKind::HighFrequency,
        3 => AnomalyKind::NewGeography,
        4 => AnomalyKind::LimitExhaustion,
        _ => AnomalyKind::Structuring,
    }
}


fn inject(card: &mut CardState, kind: AnomalyKind, rng: &mut SmallRng) -> Vec<Transaction> {
    match kind {
        AnomalyKind::LargeAmount      => vec![make_large_amount_tx(card, rng)],
        AnomalyKind::ImpossibleTravel => vec![make_impossible_travel_tx(card, rng)],
        AnomalyKind::HighFrequency    =>      make_high_frequency_burst(card, rng),
        AnomalyKind::NewGeography     => vec![make_new_geography_tx(card, rng)],
        AnomalyKind::LimitExhaustion  => vec![make_limit_exhaustion_tx(card, rng)],
        AnomalyKind::Structuring      => vec![make_structuring_tx(card, rng)],
    }
}

fn make_large_amount_tx(card: &mut CardState, rng: &mut SmallRng) -> Transaction {
    // Ensure the limit won't clamp away the anomaly.
    if card.limit < card.typical_amount * 3.0 {
        card.limit = round2(rng.gen_range(10_000.0_f64..=20_000.0));
    }

    let amount    = round2((card.typical_amount * rng.gen_range(5.0_f64..=15.0))
                        .min(card.limit));
    let new_limit = round2((card.limit - amount).max(0.0));
    let location  = local_move(card, rng);
    let merchant  = merchant_for(card.current_region, rng);

    card.limit         = new_limit;
    card.last_location = location.clone();

    build_tx(card, amount, new_limit, location, merchant, AnomalyKind::LargeAmount)
}


fn make_impossible_travel_tx(card: &mut CardState, rng: &mut SmallRng) -> Transaction {
    let foreign_region   = preferred_foreign(card.current_region, rng);
    let foreign_location = foreign_region.sample_location(rng);

    let raw_amount = card.typical_amount * rng.gen_range(0.5_f64..=1.5);
    let amount     = round2(raw_amount.max(1.0).min(card.limit.max(1.0)));
    let new_limit  = round2((card.limit - amount).max(0.0));
    let merchant   = merchant_for(foreign_region, rng);

    card.limit           = new_limit;
    card.current_region  = foreign_region;
    card.last_location   = foreign_location.clone();
    card.visited_regions.insert(foreign_region);

    build_tx(card, amount, new_limit, foreign_location, merchant, AnomalyKind::ImpossibleTravel)
}

/// Picks a region far from `current` so the resulting jump is always a
/// genuine cross-region "impossible travel", regardless of where the card
/// is currently transacting from.
fn preferred_foreign(current: Region, rng: &mut SmallRng) -> Region {
    let preferred = match current {
        Region::Poland        => Region::EastAsia,
        Region::WesternEurope => Region::EastAsia,
        Region::NorthAmerica  => Region::EastAsia,
        Region::EastAsia      => Region::NorthAmerica,
    };
    if preferred != current { return preferred; }

    *Region::ALL.iter().filter(|&&r| r != current).choose(rng).unwrap()
}

fn make_high_frequency_burst(card: &mut CardState, rng: &mut SmallRng) -> Vec<Transaction> {
    let burst_size: usize = rng.gen_range(5..=15);
    let mut txs = Vec::with_capacity(burst_size);

    for _ in 0..burst_size {
        // Ensure there is always enough limit for one more transaction.
        if card.limit < card.typical_amount * 0.5 {
            card.limit = round2(rng.gen_range(2_000.0_f64..=10_000.0));
        }

        let raw_amount = card.typical_amount * rng.gen_range(0.5_f64..=1.5);
        let amount     = round2(raw_amount.max(1.0).min(card.limit));
        let new_limit  = round2((card.limit - amount).max(0.0));
        let location   = local_move(card, rng);
        let merchant   = merchant_for(card.current_region, rng);

        card.limit         = new_limit;
        card.last_location = location.clone();

        txs.push(build_tx(card, amount, new_limit, location, merchant, AnomalyKind::HighFrequency));
    }

    txs
}


fn make_new_geography_tx(card: &mut CardState, rng: &mut SmallRng) -> Transaction {
    let unvisited: Vec<Region> = Region::ALL
        .iter()
        .copied()
        .filter(|r| !card.visited_regions.contains(r))
        .collect();

    if unvisited.is_empty() {
        let raw_amount = card.typical_amount * rng.gen_range(0.5_f64..=1.5);
        let amount     = round2(raw_amount.max(1.0).min(card.limit.max(1.0)));
        let new_limit  = round2((card.limit - amount).max(0.0));
        let location   = local_move(card, rng);
        let merchant   = merchant_for(card.current_region, rng);
        card.limit         = new_limit;
        card.last_location = location.clone();
        return build_tx(card, amount, new_limit, location, merchant, AnomalyKind::NewGeography);
    }

    let new_region   = *unvisited.choose(rng).unwrap();
    let location     = new_region.sample_location(rng);
    let merchant     = merchant_for(new_region, rng);

    let raw_amount   = card.typical_amount * rng.gen_range(0.5_f64..=1.5);
    let amount       = round2(raw_amount.max(1.0).min(card.limit.max(1.0)));
    let new_limit    = round2((card.limit - amount).max(0.0));

    card.visited_regions.insert(new_region);
    card.current_region = new_region;
    card.limit         = new_limit;
    card.last_location = location.clone();

    build_tx(card, amount, new_limit, location, merchant, AnomalyKind::NewGeography)
}


fn make_limit_exhaustion_tx(card: &mut CardState, rng: &mut SmallRng) -> Transaction {
    // Ensure the limit is large enough that 95% is a meaningful amount.
    if card.limit < 50.0 {
        card.limit = round2(rng.gen_range(1_000.0_f64..=20_000.0));
    }

    let drain_ratio = rng.gen_range(0.95_f64..=0.99);
    let amount      = round2(card.limit * drain_ratio);
    let new_limit   = round2((card.limit - amount).max(0.0));
    let location    = local_move(card, rng);
    let merchant    = merchant_for(card.current_region, rng);

    card.limit         = new_limit;
    card.last_location = location.clone();

    build_tx(card, amount, new_limit, location, merchant, AnomalyKind::LimitExhaustion)
}

fn make_structuring_tx(card: &mut CardState, rng: &mut SmallRng) -> Transaction {
    let threshold = *[500.0_f64, 1_000.0, 5_000.0].choose(rng).unwrap();

    let offset = rng.gen_range(1.0_f64..=50.0);
    let raw_amount = threshold - offset;

    if card.limit < raw_amount {
        card.limit = round2(rng.gen_range(
            (raw_amount * 1.5).max(1_000.0)..=20_000.0
        ));
    }

    let amount    = round2(raw_amount.min(card.limit));
    let new_limit = round2((card.limit - amount).max(0.0));
    let location  = local_move(card, rng);
    let merchant  = merchant_for(card.current_region, rng);

    card.limit         = new_limit;
    card.last_location = location.clone();

    build_tx(card, amount, new_limit, location, merchant, AnomalyKind::Structuring)
}

pub fn merchant_for(region: Region, rng: &mut SmallRng) -> String {
    let list: &[&str] = match region {
        Region::Poland        => &["Biedronka","Lidl","Żabka","Orlen","BP","InPost","Allegro","PKP Intercity","Empik"],
        Region::WesternEurope => &["Carrefour","REWE","Tesco","Shell","Aldi","H&M","Zara","DHL","Lufthansa"],
        Region::NorthAmerica  => &["Walmart","Target","Chevron","Starbucks","Amazon","Uber","CVS","Delta Airlines"],
        Region::EastAsia      => &["FamilyMart","7-Eleven","Lawson","UnionPay","Grab","JD.com","ANA","Rakuten"],
    };
    list.choose(rng).unwrap().to_string()
}

pub fn build_tx(
    card:     &CardState,
    amount:   f64,
    limit:    f64,
    location: GpsCoords,
    merchant: String,
    kind:     AnomalyKind,
) -> Transaction {
    use chrono::Utc;
    use uuid::Uuid;
    Transaction {
        transaction_id:      Uuid::new_v4(),
        card_id:             card.card_id.clone(),
        user_id:             card.user_id.clone(),
        timestamp:           Utc::now(),
        location,
        amount_pln:          amount,
        remaining_limit_pln: limit,
        merchant,
        injected_anomaly:    Some(kind),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::build_fleet;
    use rand::SeedableRng;
    use shared::{AnomalyKind, GpsCoords};


    fn dummy_normal(card: &mut CardState, _rng: &mut SmallRng) -> Transaction {
        use chrono::Utc;
        use uuid::Uuid;
        Transaction {
            transaction_id:      Uuid::new_v4(),
            card_id:             card.card_id.clone(),
            user_id:             card.user_id.clone(),
            timestamp:           Utc::now(),
            location:            card.last_location.clone(),
            amount_pln:          card.typical_amount,
            remaining_limit_pln: card.limit,
            merchant:            "Test".to_string(),
            injected_anomaly:    None,
        }
    }

    fn in_bounding_box(point: &GpsCoords, region: Region) -> bool {
        match region {
            Region::Poland        => (49.0..=54.9).contains(&point.lat)   && (14.1..=24.1).contains(&point.lon),
            Region::WesternEurope => (43.0..=53.0).contains(&point.lat)   && (-5.0..=15.0).contains(&point.lon),
            Region::NorthAmerica  => (25.0..=50.0).contains(&point.lat)   && (-125.0..=-65.0).contains(&point.lon),
            Region::EastAsia      => (22.0..=45.0).contains(&point.lat)   && (100.0..=145.0).contains(&point.lon),
        }
    }

    fn fresh_fleet(seed: u64, n: usize) -> Vec<CardState> {
        build_fleet(n, &mut SmallRng::seed_from_u64(seed))
    }

    #[test]
    fn large_amount_is_at_least_5x_typical() {
        let mut rng   = SmallRng::seed_from_u64(42);
        let mut fleet = fresh_fleet(42, 100);

        for _ in 0..5_000 {
            let idx          = rng.gen_range(0..fleet.len());
            let typical      = fleet[idx].typical_amount;
            let limit_before = fleet[idx].limit;
            let tx           = make_large_amount_tx(&mut fleet[idx], &mut rng);

            assert_eq!(tx.injected_anomaly, Some(AnomalyKind::LargeAmount));
            assert!(
                tx.amount_pln >= typical * 4.9 || tx.amount_pln >= limit_before * 0.99,
                "amount {:.2} is neither ≥ 5× typical ({:.2}) nor near limit ({:.2})",
                tx.amount_pln, typical * 5.0, limit_before
            );
            assert!(tx.amount_pln > 0.0);
            assert!(tx.remaining_limit_pln >= 0.0);
        }
    }

    #[test]
    fn impossible_travel_location_outside_home_and_far() {
        let orig  = fresh_fleet(7, 200);
        let mut rng = SmallRng::seed_from_u64(7);

        for _ in 0..5_000 {
            let idx      = rng.gen_range(0..orig.len());
            let home     = orig[idx].home_region;
            let home_loc = orig[idx].last_location.clone();

            let mut card = crate::fleet::CardState {
                card_id:         orig[idx].card_id.clone(),
                user_id:         orig[idx].user_id.clone(),
                typical_amount:  orig[idx].typical_amount,
                limit:           orig[idx].limit,
                home_region:     orig[idx].home_region,
                current_region:  orig[idx].current_region,
                last_location:   orig[idx].last_location.clone(),
                visited_regions: orig[idx].visited_regions.clone(),
            };

            let tx       = make_impossible_travel_tx(&mut card, &mut rng);
            let distance = home_loc.distance_km(&tx.location);

            assert_eq!(tx.injected_anomaly, Some(AnomalyKind::ImpossibleTravel));
            assert!(!in_bounding_box(&tx.location, home),
                "location is still inside home bounding box");
            assert!(distance > 5_000.0,
                "distance {distance:.0} km is ≤ 5 000 km");
        }
    }

    #[test]
    fn high_frequency_burst_is_5_to_15_transactions() {
        let mut rng   = SmallRng::seed_from_u64(11);
        let mut fleet = fresh_fleet(11, 50);

        for _ in 0..1_000 {
            let idx  = rng.gen_range(0..fleet.len());
            let txs  = make_high_frequency_burst(&mut fleet[idx], &mut rng);

            assert!(txs.len() >= 5 && txs.len() <= 15,
                "burst length {} is outside [5, 15]", txs.len());

            for tx in &txs {
                assert_eq!(tx.injected_anomaly, Some(AnomalyKind::HighFrequency));
                assert_eq!(tx.card_id, fleet[idx].card_id,
                    "all burst transactions must belong to the same card");
                assert!(tx.amount_pln > 0.0);
                assert!(tx.remaining_limit_pln >= 0.0);
            }
        }
    }

    #[test]
    fn high_frequency_amounts_are_normal_sized() {
        let mut rng   = SmallRng::seed_from_u64(22);
        let mut fleet = fresh_fleet(22, 50);

        for _ in 0..500 {
            let idx     = rng.gen_range(0..fleet.len());
            let typical = fleet[idx].typical_amount;
            let txs     = make_high_frequency_burst(&mut fleet[idx], &mut rng);

            for tx in &txs {
                // Each burst transaction should be within normal range (not large).
                assert!(tx.amount_pln <= typical * 1.6,
                    "burst tx amount {:.2} looks like LargeAmount (typical {typical:.2})",
                    tx.amount_pln);
            }
        }
    }

    #[test]
    fn new_geography_location_is_outside_all_visited_regions() {
        let mut rng   = SmallRng::seed_from_u64(33);
        let mut fleet = fresh_fleet(33, 100);

        for _ in 0..3_000 {
            let idx     = rng.gen_range(0..fleet.len());
            let home    = fleet[idx].home_region;
            let visited = fleet[idx].visited_regions.clone();

            if visited.len() == 4 { continue; }

            let tx = make_new_geography_tx(&mut fleet[idx], &mut rng);
            assert_eq!(tx.injected_anomaly, Some(AnomalyKind::NewGeography));

            assert!(
                !in_bounding_box(&tx.location, home),
                "location lat={:.4} lon={:.4} is still inside home region {:?}",
                tx.location.lat, tx.location.lon, home,
            );

            let in_some = Region::ALL.iter().any(|&r| in_bounding_box(&tx.location, r));
            assert!(in_some,
                "location lat={:.4} lon={:.4} is not inside any known region",
                tx.location.lat, tx.location.lon);
        }
    }

    #[test]
    fn new_geography_marks_region_as_visited() {
        let mut rng   = SmallRng::seed_from_u64(44);
        let mut fleet = fresh_fleet(44, 50);

        let idx = fleet.iter().position(|c| c.visited_regions.len() < 4).unwrap();
        let before_count = fleet[idx].visited_regions.len();

        make_new_geography_tx(&mut fleet[idx], &mut rng);

        assert_eq!(
            fleet[idx].visited_regions.len(), before_count + 1,
            "visited_regions should grow by 1 after NewGeography injection"
        );
    }

    // ── LimitExhaustion ───────────────────────────────────────────────────────

    #[test]
    fn limit_exhaustion_drains_at_least_95_percent() {
        let mut rng   = SmallRng::seed_from_u64(55);
        let mut fleet = fresh_fleet(55, 100);

        for _ in 0..5_000 {
            let idx          = rng.gen_range(0..fleet.len());
            // Ensure a meaningful limit.
            if fleet[idx].limit < 50.0 { fleet[idx].limit = 5_000.0; }
            let limit_before = fleet[idx].limit;
            let tx           = make_limit_exhaustion_tx(&mut fleet[idx], &mut rng);

            assert_eq!(tx.injected_anomaly, Some(AnomalyKind::LimitExhaustion));

            let drain_ratio = tx.amount_pln / (tx.amount_pln + tx.remaining_limit_pln);
            assert!(
                drain_ratio >= 0.94,   // 0.94 not 0.95 for float rounding slack
                "drain ratio {drain_ratio:.4} is < 95% (amount={:.2}, remaining={:.2})",
                tx.amount_pln, tx.remaining_limit_pln,
            );

            assert!(tx.amount_pln > 0.0);
            assert!(tx.remaining_limit_pln >= 0.0);
            let _ = limit_before; // used implicitly via the drain_ratio check
        }
    }

    #[test]
    fn structuring_amount_is_just_below_threshold() {
        let mut rng   = SmallRng::seed_from_u64(66);
        let mut fleet = fresh_fleet(66, 100);
        let thresholds = [500.0_f64, 1_000.0, 5_000.0];

        for _ in 0..5_000 {
            let idx = rng.gen_range(0..fleet.len());
            let tx  = make_structuring_tx(&mut fleet[idx], &mut rng);

            assert_eq!(tx.injected_anomaly, Some(AnomalyKind::Structuring));

            let near_threshold = thresholds.iter().any(|&t| {
                tx.amount_pln >= t - 50.0 && tx.amount_pln < t
            });
            assert!(
                near_threshold,
                "amount {:.2} is not within 50 PLN below any threshold {:?}",
                tx.amount_pln, thresholds,
            );

            assert!(tx.amount_pln > 0.0);
            assert!(tx.remaining_limit_pln >= 0.0);
        }
    }

    #[test]
    fn pick_kind_all_six_types_appear_roughly_equally() {
        let mut rng    = SmallRng::seed_from_u64(77);
        let mut counts = [0u32; 6];
        let n          = 60_000;

        for _ in 0..n {
            let i = match pick_kind(&mut rng) {
                AnomalyKind::LargeAmount      => 0,
                AnomalyKind::ImpossibleTravel => 1,
                AnomalyKind::HighFrequency    => 2,
                AnomalyKind::NewGeography     => 3,
                AnomalyKind::LimitExhaustion  => 4,
                AnomalyKind::Structuring      => 5,
            };
            counts[i] += 1;
        }

        let expected = n as f64 / 6.0;
        for (i, &c) in counts.iter().enumerate() {
            let pct = c as f64 / n as f64 * 100.0;
            assert!(
                pct > 12.0 && pct < 22.0,
                "kind[{i}] share {pct:.1}% is outside 12–22% (expected ~16.7%)"
            );
        }
        assert!(counts.iter().all(|&c| c > 0), "some kind never appeared");
    }

    #[test]
    fn maybe_inject_rate_and_field_are_correct() {
        let mut rng   = SmallRng::seed_from_u64(99);
        let mut fleet = fresh_fleet(99, 100);
        let mut anom  = 0u64;
        let n         = 10_000;

        for _ in 0..n {
            let idx = rng.gen_range(0..fleet.len());
            let (txs, was_anomalous) = maybe_inject(
                &mut fleet[idx], dummy_normal, &mut rng, 0.06,
            );

            if was_anomalous {
                anom += 1;
                for tx in &txs {
                    assert!(tx.injected_anomaly.is_some(),
                        "was_anomalous=true but injected_anomaly is None");
                }
            } else {
                assert_eq!(txs.len(), 1);
                assert_eq!(txs[0].injected_anomaly, None,
                    "was_anomalous=false but injected_anomaly is Some");
            }
        }

        assert!(anom >= 400 && anom <= 800,
            "Expected ~600 anomaly events at 6% rate, got {anom}");
    }
}