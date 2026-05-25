/// Shared data types for the card anomaly detection project.
/// This file is the single source of truth for the JSON contract
/// agreed between Student A (Rust) and Student B (Python/Flink).
///
/// JSON schema version: 1.0
/// Kafka topics:
///   "transactions"  — produced by tx-simulator, consumed by Flink + tx-inspector
///   "alerts"        — produced by Flink, consumed by alarm-watcher

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── GPS coordinates ──────────────────────────────────────────────────────────

/// WGS-84 coordinates attached to every transaction.
/// Flink will use these to detect impossible-travel anomalies.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpsCoords {
    pub lat: f64,
    pub lon: f64,
}

impl GpsCoords {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }

    /// Haversine distance in kilometres between two GPS points.
    pub fn distance_km(&self, other: &GpsCoords) -> f64 {
        const R: f64 = 6371.0;
        let dlat = (other.lat - self.lat).to_radians();
        let dlon = (other.lon - self.lon).to_radians();
        let a = (dlat / 2.0).sin().powi(2)
            + self.lat.to_radians().cos()
                * other.lat.to_radians().cos()
                * (dlon / 2.0).sin().powi(2);
        2.0 * R * a.sqrt().asin()
    }
}

// ── Transaction (Kafka topic: "transactions") ────────────────────────────────

/// One payment-card transaction.
/// Produced by tx-simulator; consumed by Flink anomaly detector and tx-inspector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Globally-unique ID for this event.
    pub transaction_id: Uuid,
    /// Card identifier (10 000 unique cards in the simulation).
    pub card_id: String,
    /// Owner of the card. One user may have multiple cards.
    pub user_id: String,
    /// When the transaction occurred (UTC, ISO-8601).
    pub timestamp: DateTime<Utc>,
    /// Merchant location.
    pub location: GpsCoords,
    /// Amount charged in PLN (positive, two decimal places).
    pub amount_pln: f64,
    /// Remaining spending limit after this transaction.
    pub remaining_limit_pln: f64,
    /// Human-readable merchant name — useful for dashboards.
    pub merchant: String,
    /// Optional: hint set by the simulator when this event is intentionally anomalous.
    /// Flink MUST NOT rely on this field for detection; it is for ground-truth evaluation only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub injected_anomaly: Option<AnomalyKind>,
}

// ── Alert (Kafka topic: "alerts") ────────────────────────────────────────────

/// Alarm raised by the Flink detector.
/// Produced by Flink (Student B); consumed by alarm-watcher (Student A).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_id: Uuid,
    /// The transaction that triggered this alert.
    pub transaction_id: Uuid,
    pub card_id: String,
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    /// Which rule fired.
    pub anomaly_kind: AnomalyKind,
    /// Human-readable explanation, e.g. "Amount 5.2σ above 30-day mean".
    pub description: String,
    /// Normalised severity: 0.0 (low) – 1.0 (critical).
    pub severity: f64,
    /// Snapshot of the transaction that caused the alert.
    pub transaction: Transaction,
}

// ── Anomaly taxonomy ─────────────────────────────────────────────────────────

/// All anomaly types that both students agree on.
/// The simulator uses these to inject anomalies;
/// Flink uses them to tag alerts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    /// Single transaction amount is far above the card's historical mean (z-score).
    LargeAmount,
    /// Card used at two locations impossibly far apart within a short time window.
    ImpossibleTravel,
    /// More transactions per minute than the card's historical baseline allows.
    HighFrequency,
    /// Transaction in a country/region the card has never been used in.
    NewGeography,
    /// Spending limit is nearly or completely exhausted in an unusually short time.
    LimitExhaustion,
    /// Many small transactions just below a round-number threshold (structuring).
    Structuring,
}

impl std::fmt::Display for AnomalyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::LargeAmount      => "Large amount",
            Self::ImpossibleTravel => "Impossible travel",
            Self::HighFrequency    => "High frequency",
            Self::NewGeography     => "New geography",
            Self::LimitExhaustion  => "Limit exhaustion",
            Self::Structuring      => "Structuring",
        };
        write!(f, "{s}")
    }
}

// ── Kafka topic constants ─────────────────────────────────────────────────────

pub const TOPIC_TRANSACTIONS: &str = "transactions";
pub const TOPIC_ALERTS:       &str = "alerts";

// ── Serialisation helpers ─────────────────────────────────────────────────────

impl Transaction {
    pub fn to_json(&self) -> anyhow::Result<String>
    where
        Self: Serialize,
    {
        Ok(serde_json::to_string(self)?)
    }
}

impl Alert {
    pub fn to_json(&self) -> anyhow::Result<String>
    where
        Self: Serialize,
    {
        Ok(serde_json::to_string(self)?)
    }
}