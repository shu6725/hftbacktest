use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{
    live::ipc::TO_ALL,
    prelude::*,
};
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
use tracing::{error, warn};

use crate::{
    bitbank::{
        BitbankError,
        msg::stream::{DepthDiff, DepthWhole, Transactions, WsMessage},
    },
    connector::PublishEvent,
    utils::parse_depth,
};

const BITBANK_WS_URL: &str = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket";

pub struct MarketDataStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
}

impl MarketDataStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
    ) -> Self {
        Self { ev_tx, symbol_rx }
    }

    fn process_depth_whole(&self, symbol: &str, data: DepthWhole) {
        let ts = data.timestamp.unwrap_or_else(|| Utc::now().timestamp_millis());
        let exch_ts = ts * 1_000_000;
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();

        match parse_depth(data.bids, data.asks) {
            Ok((bids, asks)) => {
                self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

                // Clear both sides before applying the snapshot
                self.ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                        symbol: symbol.to_string(),
                        event: Event {
                            ev: LOCAL_DEPTH_CLEAR_EVENT,
                            exch_ts,
                            local_ts,
                            order_id: 0,
                            px: 0.0,
                            qty: 0.0,
                            ival: 0,
                            fval: 0.0,
                        },
                    }))
                    .unwrap();

                for (px, qty) in bids {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.to_string(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts,
                                local_ts,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                for (px, qty) in asks {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.to_string(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts,
                                local_ts,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
            }
            Err(e) => {
                error!(error = ?e, %symbol, "Couldn't parse depth_whole.");
            }
        }
    }

    fn process_depth_diff(&self, symbol: &str, data: DepthDiff) {
        let exch_ts = data.t * 1_000_000;
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();

        match parse_depth(data.bids, data.asks) {
            Ok((bids, asks)) => {
                self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

                for (px, qty) in bids {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.to_string(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts,
                                local_ts,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                for (px, qty) in asks {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.to_string(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts,
                                local_ts,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
            }
            Err(e) => {
                error!(error = ?e, %symbol, "Couldn't parse depth_diff.");
            }
        }
    }

    fn process_transactions(&self, symbol: &str, data: Transactions) {
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
        for tx in data.transactions {
            let ev = if tx.side == hftbacktest::types::Side::Buy {
                LOCAL_BUY_TRADE_EVENT
            } else {
                LOCAL_SELL_TRADE_EVENT
            };
            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: symbol.to_string(),
                    event: Event {
                        ev,
                        exch_ts: tx.executed_at * 1_000_000,
                        local_ts,
                        order_id: tx.transaction_id as u64,
                        px: tx.price,
                        qty: tx.amount,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
        }
    }

    fn handle_message(&self, text: &str) -> Result<(), BitbankError> {
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
        let room = &msg.room_name;

        if let Some(symbol) = room.strip_prefix("depth_whole_") {
            let data: DepthWhole = serde_json::from_value(msg.message.data)?;
            self.process_depth_whole(symbol, data);
        } else if let Some(symbol) = room.strip_prefix("depth_diff_") {
            let data: DepthDiff = serde_json::from_value(msg.message.data)?;
            self.process_depth_diff(symbol, data);
        } else if let Some(symbol) = room.strip_prefix("transactions_") {
            let data: Transactions = serde_json::from_value(msg.message.data)?;
            self.process_transactions(symbol, data);
        }

        Ok(())
    }

    pub async fn connect(&mut self, rooms: &[String]) -> Result<(), BitbankError> {
        let request = BITBANK_WS_URL.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();

        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();
        let mut rooms_joined = false;

        loop {
            select! {
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(60) {
                        warn!("bitbank market data stream: ping timeout");
                        return Err(BitbankError::ConnectionInterrupted);
                    }
                }
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            for suffix in ["depth_whole_", "depth_diff_", "transactions_"] {
                                let room = format!("{suffix}{symbol}");
                                let m = format!("42[\"join-room\",\"{room}\"]");
                                write.send(Message::Text(m.into())).await?;
                            }
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
                        } else if s.starts_with("40") && !rooms_joined {
                            // Socket.IO CONNECTED → join initial rooms
                            for room in rooms {
                                let m = format!("42[\"join-room\",\"{room}\"]");
                                write.send(Message::Text(m.into())).await?;
                            }
                            rooms_joined = true;
                        } else if s == "2" {
                            // Engine.IO PING → reply PONG
                            write.send(Message::Text("3".into())).await?;
                            last_ping = Instant::now();
                        } else if s.starts_with("42") {
                            if let Err(e) = self.handle_message(s) {
                                error!(error = ?e, "market_data_stream: parse error");
                            }
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
