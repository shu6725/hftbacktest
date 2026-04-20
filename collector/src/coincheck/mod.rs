mod http;

use chrono::{DateTime, Utc};
use http::keep_connection;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::error::ConnectorError;

fn extract_symbol(j: &serde_json::Value) -> Option<String> {
    if let Some(arr) = j.as_array() {
        if let Some(first) = arr.first() {
            // Orderbook: ["btc_jpy", {"bids": [...], "asks": [...], "last_update_at": "..."}]
            if let Some(sym) = first.as_str() {
                return Some(sym.to_string());
            }
            // Trades: [["timestamp", "id", "btc_jpy", "rate", "amount", "side", ...], ...]
            if let Some(inner) = first.as_array() {
                return inner.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());
            }
        }
    }

    None
}

fn handle(
    writer_tx: &UnboundedSender<(DateTime<Utc>, String, String)>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;

    let symbol = match extract_symbol(&j) {
        Some(s) => s,
        None => return Ok(()), // subscription confirmation or unknown message
    };

    let _ = writer_tx.send((recv_time, symbol, data.to_string()));
    Ok(())
}

pub async fn run_collection(
    channel_templates: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
) -> Result<(), anyhow::Error> {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel();
    let h = tokio::spawn(keep_connection(channel_templates, symbols, ws_tx.clone()));

    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(&writer_tx, recv_time, data) {
            error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = h.await;
    Ok(())
}
