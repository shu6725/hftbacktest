mod http;

use chrono::{DateTime, Utc};
use http::keep_connection;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::error::ConnectorError;

// Room name prefixes used by bitbank's Socket.IO API
const ROOM_PREFIXES: &[&str] = &["depth_whole_", "depth_diff_", "transactions_"];

fn extract_symbol(room_name: &str) -> Option<&str> {
    for prefix in ROOM_PREFIXES {
        if let Some(sym) = room_name.strip_prefix(prefix) {
            return Some(sym);
        }
    }
    None
}

fn handle(
    writer_tx: &UnboundedSender<(DateTime<Utc>, String, String)>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    // bitbank Socket.IO events look like:
    //   42["message",{"room_name":"depth_whole_btc_jpy","message":{"data":{...}}}]
    let s = data.as_str();

    // Strip the "42" Socket.IO/Engine.IO prefix
    let json_str = s.strip_prefix("42").ok_or(ConnectorError::FormatError)?;

    let j: serde_json::Value = serde_json::from_str(json_str)?;
    let arr = j.as_array().ok_or(ConnectorError::FormatError)?;

    // arr[0] is the event name; we only care about "message" events
    let event = arr
        .first()
        .and_then(|v| v.as_str())
        .ok_or(ConnectorError::FormatError)?;
    if event != "message" {
        return Ok(());
    }

    let payload = arr.get(1).ok_or(ConnectorError::FormatError)?;
    let room_name = payload
        .get("room_name")
        .and_then(|v| v.as_str())
        .ok_or(ConnectorError::FormatError)?;

    let symbol = extract_symbol(room_name).ok_or(ConnectorError::FormatError)?;

    let _ = writer_tx.send((recv_time, symbol.to_string(), data.to_string()));
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
