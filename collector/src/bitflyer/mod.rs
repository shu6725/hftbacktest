mod http;

use chrono::{DateTime, Utc};
use http::keep_connection;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::error::ConnectorError;

const CHANNEL_PREFIXES: &[&str] = &[
    "lightning_board_snapshot_",
    "lightning_board_",
    "lightning_executions_",
    "lightning_ticker_",
];

fn extract_symbol(channel: &str) -> Option<&str> {
    for prefix in CHANNEL_PREFIXES {
        if let Some(sym) = channel.strip_prefix(prefix) {
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
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;

    // Skip subscription responses (they have "result" field, not "method")
    let method = match j.get("method").and_then(|m| m.as_str()) {
        Some(m) => m,
        None => return Ok(()),
    };

    if method != "channelMessage" {
        return Ok(());
    }

    let params = j.get("params").ok_or(ConnectorError::FormatError)?;
    let channel = params
        .get("channel")
        .and_then(|c| c.as_str())
        .ok_or(ConnectorError::FormatError)?;

    let symbol = extract_symbol(channel).ok_or(ConnectorError::FormatError)?;

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
