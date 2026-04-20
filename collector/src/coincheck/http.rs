use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Utf8Bytes, client::IntoClientRequest},
};
use tracing::{error, warn};

const COINCHECK_WS_URL: &str = "wss://ws-api.coincheck.com";

pub async fn connect(
    channels: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) -> Result<(), anyhow::Error> {
    let request = COINCHECK_WS_URL.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();

    for channel in channels.iter() {
        let msg = serde_json::json!({
            "type": "subscribe",
            "channel": channel,
        });
        write
            .send(Message::Text(msg.to_string().into()))
            .await?;
    }

    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let recv_time = Utc::now();
                if ws_tx.send((recv_time, text)).is_err() {
                    break;
                }
            }
            Some(Ok(Message::Binary(_))) => {}
            Some(Ok(Message::Ping(data))) => {
                if write.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(close_frame))) => {
                warn!(?close_frame, "connection closed");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "connection closed",
                )));
            }
            Some(Ok(Message::Frame(_))) => {}
            Some(Err(e)) => {
                return Err(Error::from(e));
            }
            None => {
                break;
            }
        }
    }
    Ok(())
}

pub async fn keep_connection(
    channel_templates: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) {
    let channels: Vec<String> = symbol_list
        .iter()
        .flat_map(|symbol| {
            channel_templates
                .iter()
                .map(|tmpl| tmpl.replace("$symbol", symbol.as_str()))
                .collect::<Vec<_>>()
        })
        .collect();

    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        if let Err(error) = connect(channels.clone(), ws_tx.clone()).await {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }

            let sleep_duration = if error_count > 20 {
                Duration::from_secs(10)
            } else if error_count > 10 {
                Duration::from_secs(5)
            } else if error_count > 3 {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(500)
            };

            tokio::time::sleep(sleep_duration).await;
        } else {
            break;
        }
    }
}
