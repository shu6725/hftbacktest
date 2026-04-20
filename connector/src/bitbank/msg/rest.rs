use hftbacktest::types::{OrdType, Side, Status};
use serde::Deserialize;

use super::{from_str_to_side, from_str_to_status, from_str_to_type};
use crate::utils::from_str_to_f64;

/// Wrapper for all bitbank API responses: `{"success": 1, "data": T}` or `{"success": 0, "data": {"code": N}}`
#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    pub success: i32,
    pub data: T,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderResponse {
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

#[derive(Debug, Deserialize)]
pub struct ErrorData {
    pub code: i64,
}

#[derive(Debug, Deserialize)]
pub struct AssetEntry {
    pub asset: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub free_amount: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub locked_amount: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub onhand_amount: f64,
}

#[derive(Debug, Deserialize)]
pub struct AssetsData {
    pub assets: Vec<AssetEntry>,
}
