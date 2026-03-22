use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Utf8Bytes, client::IntoClientRequest},
};
use tracing::{error, warn};

const COINBASE_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

pub async fn connect(
    product_ids: Vec<String>,
    channels: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) -> Result<(), anyhow::Error> {
    let request = COINBASE_WS_URL.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let (_ping_tx, mut ping_rx) = unbounded_channel::<()>();

    // Subscribe to each channel with all product_ids
    for channel in &channels {
        let msg = serde_json::json!({
            "type": "subscribe",
            "product_ids": product_ids,
            "channel": channel,
        });
        write
            .send(Message::Text(msg.to_string().into()))
            .await?;
    }

    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            select! {
                _ = ping_interval.tick() => {
                    // Coinbase doesn't require explicit pings; WebSocket ping/pong is handled
                    // by the library. Just keep the loop alive.
                }
                result = ping_rx.recv() => {
                    if result.is_none() {
                        break;
                    }
                }
            }
        }
    });

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
    channels: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();

        if let Err(error) = connect(symbol_list.clone(), channels.clone(), ws_tx.clone()).await {
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
