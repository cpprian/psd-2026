use rand::prelude::*;
use rand::rngs::SmallRng;
use shared::GpsCoords;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    Poland,
    WesternEurope,
    NorthAmerica,
    EastAsia,
}

impl Region {
    pub const ALL: [Region; 4] = [
        Self::Poland,
        Self::WesternEurope,
        Self::NorthAmerica,
        Self::EastAsia,
    ];

    pub fn random(rng: &mut SmallRng) -> Self {
        match rng.gen_range(0..4) {
            0 => Self::Poland,
            1 => Self::WesternEurope,
            2 => Self::NorthAmerica,
            _ => Self::EastAsia,
        }
    }

    pub fn sample_location(&self, rng: &mut SmallRng) -> GpsCoords {
        let (lat, lon) = match self {
            Self::Poland        => (49.0_f64..54.9,  14.1..24.1),
            Self::WesternEurope => (43.0..53.0,      -5.0..15.0),
            Self::NorthAmerica  => (25.0..50.0,    -125.0..-65.0),
            Self::EastAsia      => (22.0..45.0,     100.0..145.0),
        };
        GpsCoords::new(rng.gen_range(lat), rng.gen_range(lon))
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Poland        => "Poland",
            Self::WesternEurope => "Western Europe",
            Self::NorthAmerica  => "North America",
            Self::EastAsia      => "East Asia",
        }
    }
}

#[derive(Debug)]
pub struct CardState {
    pub card_id:         String,
    pub user_id:         String,
    pub typical_amount:  f64,
    pub limit:           f64,
    pub home_region:     Region,
    pub last_location:   GpsCoords,
    pub visited_regions: HashSet<Region>,
}

pub fn build_fleet(n: usize, rng: &mut SmallRng) -> Vec<CardState> {
    let n_users = ((n as f64) / 1.4) as usize;

    (0..n)
        .map(|i| {
            let region   = Region::random(rng);
            let location = region.sample_location(rng);

            let log_min = 10_f64.ln();
            let log_max = 800_f64.ln();
            let typical = (log_min + rng.gen::<f64>() * (log_max - log_min)).exp();
            let limit   = (typical * rng.gen_range(10.0_f64..50.0)).clamp(1_000.0, 20_000.0);

            let mut visited = HashSet::new();
            visited.insert(region);

            CardState {
                card_id:         format!("CARD-{i:06}"),
                user_id:         format!("USER-{:06}", i % n_users),
                typical_amount:  round2(typical),
                limit:           round2(limit),
                home_region:     region,
                last_location:   location,
                visited_regions: visited,
            }
        })
        .collect()
}

/// Round to 2 decimal places.
pub fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}