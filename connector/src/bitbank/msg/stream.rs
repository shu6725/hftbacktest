use hftbacktest::types::{OrdType, Side, Status};
use serde::Deserialize;

use super::{from_str_to_side, from_str_to_status, from_str_to_type};
use crate::utils::from_str_to_f64;

/// Top-level Socket.IO message after stripping the "42" prefix:
/// `["message", {"room_name": "...", "message": {"data": {...}}}]`
#[derive(Debug, Deserialize)]
pub struct WsMessage {
    pub room_name: String,
    pub message: WsInner,
}

#[derive(Debug, Deserialize)]
pub struct WsInner {
    pub data: serde_json::Value,
}

// ── Market data types ────────────────────────────────────────────────────────

/// `depth_whole_$pair` — full order book snapshot
#[derive(Debug, Deserialize)]
pub struct DepthWhole {
    pub asks: Vec<(String, String)>,
    pub bids: Vec<(String, String)>,
    #[serde(rename = "sequenceId")]
    pub sequence_id: String,
    /// Millisecond timestamp (may be absent in some versions)
    pub timestamp: Option<i64>,
}

/// `depth_diff_$pair` — incremental order book update
#[derive(Debug, Deserialize)]
pub struct DepthDiff {
    /// Changed ask levels; qty "0" means remove the level
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
    /// Changed bid levels; qty "0" means remove the level
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "sequenceId")]
    pub sequence_id: String,
    /// Millisecond exchange timestamp
    pub t: i64,
}

/// `transactions_$pair` — trade execution feed
#[derive(Debug, Deserialize)]
pub struct Transactions {
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    pub transaction_id: i64,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub amount: f64,
    pub executed_at: i64,
}

// ── Private order update ─────────────────────────────────────────────────────

/// `spot_order_$pair` — order status update
#[derive(Debug, Deserialize, Clone)]
pub struct SpotOrder {
    pub order_id: i64,
    pub pair: String,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "type", deserialize_with = "from_str_to_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub start_amount: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub remaining_amount: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_amount: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub average_price: f64,
    pub ordered_at: i64,
    pub executed_at: Option<i64>,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
}
