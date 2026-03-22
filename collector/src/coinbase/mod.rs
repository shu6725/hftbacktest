mod http;

use chrono::{DateTime, Utc};
use http::keep_connection;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::error::ConnectorError;

fn handle(
    writer_tx: &UnboundedSender<(DateTime<Utc>, String, String)>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;

    let channel = match j.get("channel").and_then(|c| c.as_str()) {
        Some(c) => c,
        None => return Ok(()),
    };

    // Skip subscription confirmations and heartbeats
    if channel == "subscriptions" || channel == "heartbeats" {
        return Ok(());
    }

    let events = match j.get("events").and_then(|e| e.as_array()) {
        Some(e) if !e.is_empty() => e,
        _ => return Ok(()),
    };

    let first_event = &events[0];

    let symbol = match channel {
        "market_trades" => first_event
            .get("trades")
            .and_then(|t| t.as_array())
            .and_then(|t| t.first())
            .and_then(|t| t.get("product_id"))
            .and_then(|p| p.as_str())
            .ok_or(ConnectorError::FormatError)?,
        "l2_data" => first_event
            .get("product_id")
            .and_then(|p| p.as_str())
            .ok_or(ConnectorError::FormatError)?,
        "ticker" | "ticker_batch" => first_event
            .get("tickers")
            .and_then(|t| t.as_array())
            .and_then(|t| t.first())
            .and_then(|t| t.get("product_id"))
            .and_then(|p| p.as_str())
            .ok_or(ConnectorError::FormatError)?,
        _ => return Ok(()),
    };

    let _ = writer_tx.send((recv_time, symbol.to_string(), data.to_string()));
    Ok(())
}

pub async fn run_collection(
    channels: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
) -> Result<(), anyhow::Error> {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel();
    let h = tokio::spawn(keep_connection(channels, symbols, ws_tx.clone()));

    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(&writer_tx, recv_time, data) {
            error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = h.await;
    Ok(())
}
