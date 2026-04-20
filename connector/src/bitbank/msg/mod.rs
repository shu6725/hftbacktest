#[allow(dead_code)]
pub mod rest;
#[allow(dead_code)]
pub mod stream;

use hftbacktest::types::{OrdType, Side, Status};
use serde::{
    Deserialize,
    Deserializer,
    de::{Error, Unexpected},
};

pub fn from_str_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        s => Err(Error::invalid_value(Unexpected::Str(s), &"buy or sell")),
    }
}

pub fn from_str_to_status<'de, D>(deserializer: D) -> Result<Status, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s {
        "UNFILLED" => Ok(Status::New),
        "PARTIALLY_FILLED" => Ok(Status::PartiallyFilled),
        "FULLY_FILLED" => Ok(Status::Filled),
        "CANCELED_UNFILLED" | "CANCELED_PARTIALLY_FILLED" => Ok(Status::Canceled),
        s => Err(Error::invalid_value(
            Unexpected::Str(s),
            &"UNFILLED,PARTIALLY_FILLED,FULLY_FILLED,CANCELED_UNFILLED,CANCELED_PARTIALLY_FILLED",
        )),
    }
}

pub fn from_str_to_type<'de, D>(deserializer: D) -> Result<OrdType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s {
        "limit" | "stop" | "stop_limit" => Ok(OrdType::Limit),
        "market" => Ok(OrdType::Market),
        s => Err(Error::invalid_value(
            Unexpected::Str(s),
            &"limit,market,stop,stop_limit",
        )),
    }
}
