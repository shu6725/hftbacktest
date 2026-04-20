mod market_data_stream;
mod msg;
mod ordermanager;
mod rest;
mod user_data_stream;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use hftbacktest::{
    prelude::get_precision,
    types::{ErrorKind, LiveError, LiveEvent, Order, Status, Value},
};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tokio_tungstenite::tungstenite;
use tracing::{error, warn};

use crate::{
    bitbank::{
        market_data_stream::MarketDataStream,
        ordermanager::{OrderManager, SharedOrderManager},
        rest::BitbankClient,
        user_data_stream::UserDataStream,
    },
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry},
};

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Error, Debug)]
pub enum BitbankError {
    #[error("InstrumentNotFound")]
    InstrumentNotFound,
    #[error("InvalidRequest: {0}")]
    InvalidRequest(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("AuthFailed")]
    AuthFailed,
    #[error("FormatError")]
    FormatError,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("ApiError: {code}")]
    ApiError { code: i64 },
    #[error("OrderError: {code} - {msg}")]
    OrderError { code: i64, msg: String },
    #[error("ReqError: {0:?}")]
    ReqError(#[from] reqwest::Error),
    #[error("JsonError: {0:?}")]
    JsonError(#[from] serde_json::Error),
    #[error("Tungstenite: {0:?}")]
    Tungstenite(#[from] tungstenite::Error),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl From<BitbankError> for Value {
    fn from(e: BitbankError) -> Value {
        match e {
            BitbankError::OrderError { code, msg } => {
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(code));
                map.insert("msg".to_string(), Value::String(msg));
                Value::Map(map)
            }
            other => Value::String(other.to_string()),
        }
    }
}

// ── Config ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct Config {
    #[serde(default = "default_api_url")]
    api_url: String,
    #[serde(default = "default_ws_url")]
    ws_url: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    secret: String,
}

fn default_api_url() -> String {
    "https://api.bitbank.cc".to_string()
}

fn default_ws_url() -> String {
    "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket".to_string()
}

// ── Shared types ─────────────────────────────────────────────────────────────

pub type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

// ── Connector struct ─────────────────────────────────────────────────────────

/// Live connector for bitbank spot trading.
pub struct BitbankSpot {
    config: Config,
    symbols: SharedSymbolSet,
    order_manager: SharedOrderManager,
    client: BitbankClient,
    /// Broadcast channel used to notify running streams of newly registered symbols.
    symbol_tx: Sender<String>,
}

impl BitbankSpot {
    fn start_market_data_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let symbols: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        // Build the initial room list from already-registered symbols
        let rooms: Vec<String> = symbols
            .iter()
            .flat_map(|s| {
                [
                    format!("depth_whole_{s}"),
                    format!("depth_diff_{s}"),
                    format!("transactions_{s}"),
                ]
            })
            .collect();

        let symbol_rx = self.symbol_tx.subscribe();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|e: BitbankError| {
                    error!(error = ?e, "market_data_stream error");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            e.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = MarketDataStream::new(ev_tx.clone(), symbol_rx.resubscribe());
                    stream.connect(&rooms).await
                })
                .await;
        });
    }

    fn start_user_data_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        if self.config.api_key.is_empty() || self.config.secret.is_empty() {
            return;
        }

        let symbols: HashSet<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let shared_symbols = self.symbols.clone();
        let symbol_rx = self.symbol_tx.subscribe();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|e: BitbankError| {
                    error!(error = ?e, "user_data_stream error");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            e.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = UserDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        shared_symbols.clone(),
                        symbol_rx.resubscribe(),
                    );
                    stream.connect(&symbols).await
                })
                .await;
        });
    }
}

// ── ConnectorBuilder ─────────────────────────────────────────────────────────

impl ConnectorBuilder for BitbankSpot {
    type Error = BitbankError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        let client = BitbankClient::new(&config.api_url, &config.api_key, &config.secret);
        let order_manager = Arc::new(Mutex::new(OrderManager::new()));
        let (symbol_tx, _) = broadcast::channel(500);

        Ok(BitbankSpot {
            config,
            symbols: Default::default(),
            order_manager,
            client,
            symbol_tx,
        })
    }
}

// ── Connector trait ───────────────────────────────────────────────────────────

impl Connector for BitbankSpot {
    fn register(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            let _ = self.symbol_tx.send(symbol);
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.start_market_data_stream(ev_tx.clone());
        self.start_user_data_stream(ev_tx);
    }

    fn submit(&self, symbol: String, mut order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        // Register as pending; bail if already in-flight (duplicate)
        if !order_manager
            .lock()
            .unwrap()
            .prepare(symbol.clone(), order.clone())
        {
            warn!(order_id = order.order_id, "Duplicate submit — dropping.");
            order.req = Status::None;
            order.status = Status::Expired;
            tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                symbol,
                order,
            }))
            .unwrap();
            return;
        }

        tokio::spawn(async move {
            let result = client
                .submit_order(
                    &symbol,
                    order.side,
                    order.price_tick as f64 * order.tick_size,
                    get_precision(order.tick_size),
                    order.qty,
                    get_precision(order.tick_size), // qty precision same as price for simplicity
                    order.order_type,
                )
                .await;

            match result {
                Ok(resp) => {
                    if let Some(updated) = order_manager
                        .lock()
                        .unwrap()
                        .update_from_submit(&symbol, order.order_id, &resp)
                    {
                        tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                            symbol,
                            order: updated,
                        }))
                        .unwrap();
                    }
                }
                Err(e) => {
                    if let Some(updated) = order_manager
                        .lock()
                        .unwrap()
                        .update_submit_fail(&symbol, order.order_id)
                    {
                        tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                            symbol: symbol.clone(),
                            order: updated,
                        }))
                        .unwrap();
                    }
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        BitbankError::from(e).into(),
                    ))))
                    .unwrap();
                }
            }
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        let bitbank_order_id = order_manager
            .lock()
            .unwrap()
            .get_bitbank_order_id(&symbol, order.order_id);

        let Some(bitbank_order_id) = bitbank_order_id else {
            warn!(
                order_id = order.order_id,
                "bitbank_order_id not found; order may already be settled."
            );
            return;
        };

        tokio::spawn(async move {
            match client.cancel_order(&symbol, bitbank_order_id).await {
                Ok(resp) => {
                    if let Some(updated) = order_manager
                        .lock()
                        .unwrap()
                        .update_from_cancel(&symbol, order.order_id, &resp)
                    {
                        tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                            symbol,
                            order: updated,
                        }))
                        .unwrap();
                    }
                }
                Err(e) => {
                    if let Some(updated) = order_manager
                        .lock()
                        .unwrap()
                        .update_cancel_fail(&symbol, order.order_id, &e)
                    {
                        tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                            symbol: symbol.clone(),
                            order: updated,
                        }))
                        .unwrap();
                    }
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        e.into(),
                    ))))
                    .unwrap();
                }
            }
        });
    }
}
