use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpsCoords {
    pub lat: f64,
    pub lon: f64,
}

impl GpsCoords {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub transaction_id: Uuid,
    pub card_id: String,
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    pub location: GpsCoords,
    pub amount_pln: f64,
    pub remaining_limit_pln: f64,
    pub merchant: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub injected_anomaly: Option<AnomalyKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_id: Uuid,
    pub transaction_id: Uuid,
    pub card_id: String,
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    pub anomaly_kind: AnomalyKind,
    pub description: String,
    pub severity: f64,
    pub transaction: Transaction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    LargeAmount,
    ImpossibleTravel,
    HighFrequency,
    NewGeography,
    LimitExhaustion,
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

pub const TOPIC_TRANSACTIONS: &str = "transactions";
pub const TOPIC_ALERTS:       &str = "alerts";

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