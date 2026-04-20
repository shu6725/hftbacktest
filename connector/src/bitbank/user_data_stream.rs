use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::*;
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, warn};

use crate::{
    bitbank::{
        BitbankError,
        SharedSymbolSet,
        msg::stream::{SpotOrder, WsMessage},
        ordermanager::SharedOrderManager,
        rest::BitbankClient,
    },
    connector::PublishEvent,
    utils::sign_hmac_sha256,
};

const BITBANK_WS_URL: &str = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket";
/// The path used for WebSocket private channel authentication
const BITBANK_WS_AUTH_PATH: &str = "/v1/user/subscribe";

pub struct UserDataStream {
    client: BitbankClient,
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    symbols: SharedSymbolSet,
    symbol_rx: Receiver<String>,
}

impl UserDataStream {
    pub fn new(
        client: BitbankClient,
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        symbols: SharedSymbolSet,
        symbol_rx: Receiver<String>,
    ) -> Self {
        Self {
            client,
            ev_tx,
            order_manager,
            symbols,
            symbol_rx,
        }
    }

    fn build_auth_message(api_key: &str, secret: &str) -> String {
        let nonce = Utc::now().timestamp_millis().to_string();
        let signature = sign_hmac_sha256(secret, &format!("{nonce}{BITBANK_WS_AUTH_PATH}"));
        format!(
            r#"42["auth",{{"api_key":"{api_key}","signature":"{signature}","nonce":"{nonce}"}}]"#
        )
    }

    fn process_message(&self, text: &str) -> Result<(), BitbankError> {
        let json_str = text.strip_prefix("42").ok_or(BitbankError::FormatError)?;
        let arr: serde_json::Value = serde_json::from_str(json_str)?;
        let arr = arr.as_array().ok_or(BitbankError::FormatError)?;

        let event_name = arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or(BitbankError::FormatError)?;
        if event_name != "message" {
            return Ok(());
        }

        let payload = arr.get(1).ok_or(BitbankError::FormatError)?;
        let msg: WsMessage = serde_json::from_value(payload.clone())?;

        if let Some(pair) = msg.room_name.strip_prefix("spot_order_") {
            let ws_order: SpotOrder = serde_json::from_value(msg.message.data)?;
            match self.order_manager.lock().unwrap().update_from_ws(&ws_order) {
                Ok(Some(order)) => {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Order {
                            symbol: pair.to_string(),
                            order,
                        }))
                        .unwrap();
                }
                Ok(None) => {}
                Err(BitbankError::OrderNotFound) => {
                    // Order was created by another session — ignore
                }
                Err(e) => {
                    error!(error = ?e, ?ws_order, "user_data_stream: order update error");
                }
            }
        }

        Ok(())
    }

    pub async fn connect(&mut self, symbols: &HashSet<String>) -> Result<(), BitbankError> {
        let request = BITBANK_WS_URL.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();

        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();
        let mut authenticated = false;
        let mut _rooms_joined = false;

        // Cancel existing orders and fetch initial positions before entering the loop
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let ev_tx = self.ev_tx.clone();
        let symbols_clone = symbols.clone();
        tokio::spawn(async move {
            for pair in &symbols_clone {
                if let Err(e) = cancel_all(
                    client.clone(),
                    pair.clone(),
                    order_manager.clone(),
                    ev_tx.clone(),
                )
                .await
                {
                    error!(error = ?e, %pair, "Couldn't cancel all orders on startup.");
                }
            }
            if let Err(e) =
                fetch_positions(client.clone(), symbols_clone, ev_tx.clone()).await
            {
                error!(error = ?e, "Couldn't fetch initial positions.");
            }
        });

        loop {
            select! {
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(60) {
                        warn!("bitbank user data stream: ping timeout");
                        return Err(BitbankError::ConnectionInterrupted);
                    }
                }
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            if authenticated {
                                let room = format!("spot_order_{symbol}");
                                let m = format!("42[\"join-room\",\"{room}\"]");
                                write.send(Message::Text(m.into())).await?;
                            }
                            let client = self.client.clone();
                            let order_manager = self.order_manager.clone();
                            let ev_tx = self.ev_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = cancel_all(client, symbol, order_manager, ev_tx).await {
                                    error!(error = ?e, "cancel_all for new symbol failed");
                                }
                            });
                        }
                        Err(RecvError::Closed) => return Ok(()),
                        Err(RecvError::Lagged(n)) => error!("{n} symbol subscription(s) missed"),
                    }
                }
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        let s = text.as_str();

                        if s.starts_with('0') {
                            // Engine.IO OPEN → send Socket.IO CONNECT
                            write.send(Message::Text("40".into())).await?;
                        } else if s.starts_with("40") && !authenticated {
                            // Socket.IO CONNECTED → authenticate
                            let auth = Self::build_auth_message(
                                &self.client.api_key,
                                &self.client.secret,
                            );
                            write.send(Message::Text(auth.into())).await?;
                        } else if s.starts_with("42") {
                            // Check for auth result
                            if !authenticated {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(
                                    s.strip_prefix("42").unwrap_or(""),
                                ) {
                                    if let Some(arr) = v.as_array() {
                                        if arr.first().and_then(|v| v.as_str()) == Some("auth") {
                                            let success = arr
                                                .get(1)
                                                .and_then(|v| v.as_bool())
                                                .unwrap_or(false);
                                            if success {
                                                debug!("bitbank WS auth succeeded");
                                                authenticated = true;
                                                // Join order rooms for all already-registered symbols
                                                let syms: Vec<String> = self
                                                    .symbols
                                                    .lock()
                                                    .unwrap()
                                                    .iter()
                                                    .cloned()
                                                    .collect();
                                                for sym in &syms {
                                                    let room = format!("spot_order_{sym}");
                                                    let m = format!("42[\"join-room\",\"{room}\"]");
                                                    write
                                                        .send(Message::Text(m.into()))
                                                        .await?;
                                                }
                                            } else {
                                                error!("bitbank WS auth failed");
                                                return Err(BitbankError::AuthFailed);
                                            }
                                            continue;
                                        }
                                    }
                                }
                            }
                            if let Err(e) = self.process_message(s) {
                                error!(error = ?e, "user_data_stream: parse error");
                            }
                        } else if s == "2" {
                            write.send(Message::Text("3".into())).await?;
                            last_ping = Instant::now();
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                        last_ping = Instant::now();
                    }
                    Some(Ok(Message::Close(f))) => {
                        return Err(BitbankError::ConnectionAbort(
                            f.map(|f| f.to_string()).unwrap_or_default(),
                        ));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(BitbankError::from(e)),
                    None => return Err(BitbankError::ConnectionInterrupted),
                }
            }
        }
    }
}

pub async fn cancel_all(
    client: BitbankClient,
    pair: String,
    order_manager: SharedOrderManager,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), BitbankError> {
    client.cancel_all_orders(&pair).await?;
    let orders = order_manager.lock().unwrap().cancel_all_from_rest(&pair);
    for order in orders {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order {
                symbol: pair.clone(),
                order,
            }))
            .unwrap();
    }
    Ok(())
}

async fn fetch_positions(
    client: BitbankClient,
    mut pairs: HashSet<String>,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), BitbankError> {
    let assets = client.get_assets().await?;
    let exch_ts = Utc::now().timestamp_nanos_opt().unwrap();

    for asset in assets.assets {
        // bitbank asset names are lowercase (e.g. "btc", "jpy")
        // pairs are like "btc_jpy"; we match the base asset
        let qty = asset.free_amount;
        pairs.retain(|pair| {
            // e.g. "btc_jpy" starts with "btc"
            if pair.starts_with(&asset.asset) {
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Position {
                        symbol: pair.clone(),
                        qty,
                        exch_ts,
                    }))
                    .unwrap();
                false
            } else {
                true
            }
        });
    }

    // Emit zero position for any pairs not returned by the assets endpoint
    for pair in pairs {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                symbol: pair,
                qty: 0.0,
                exch_ts: 0,
            }))
            .unwrap();
    }
    Ok(())
}
