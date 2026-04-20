//! GMO Coin BTC_JPY live connector (Phase-1 / SIM mode).
//!
//! Subscribes to the public WebSocket feed (`orderbooks` + `trades`),
//! publishes [`LiveEvent`]s via Iceoryx2, records raw frames to disk,
//! and simulates fills from 5-second OHLCV bars.
//!
//! Usage:
//!   cargo run --release -p connector-gmocoin -- gmocoin gmocoin config/gmocoin.sim.toml

use std::{
    collections::{HashMap, hash_map::Entry},
    fs::{self, read_to_string},
    io::Write,
    panic,
    path::{Path, PathBuf},
    process::exit,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use chrono::Utc;
use clap::Parser;
use flate2::{Compression, write::GzEncoder};
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{
    live::ipc::{
        TO_ALL,
        iceoryx::{ChannelError, IceoryxBuilder},
    },
    prelude::*,
    types::{ErrorKind, LiveError, LiveEvent, Order, Side, Status},
};
use iceoryx2::{
    node::NodeBuilder,
    prelude::{SignalHandlingMode, ipc},
};
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    runtime::Builder,
    select,
    signal,
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{error, info, warn};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Error, Debug)]
pub enum GmoError {
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("FormatError: {0}")]
    FormatError(String),
    #[error("WsError: {0:?}")]
    WsError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("JsonError: {0:?}")]
    JsonError(#[from] serde_json::Error),
    #[error("IoError: {0:?}")]
    IoError(#[from] std::io::Error),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl From<GmoError> for hftbacktest::types::Value {
    fn from(e: GmoError) -> Self {
        hftbacktest::types::Value::String(e.to_string())
    }
}

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug, Clone)]
pub struct InstrumentConfig {
    pub symbol:    String,
    pub tick_size: f64,
    pub lot_size:  f64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "default_exec_mode")]
    pub exec_mode: String,
    #[serde(default)]
    pub enable_private: bool,
    #[serde(default = "default_ws_url")]
    pub public_stream_url: String,
    #[serde(default)]
    pub raw_recorder: bool,
    #[serde(default = "default_raw_dir")]
    pub raw_dir: String,
    #[serde(default)]
    pub instruments: Vec<InstrumentConfig>,
}

fn default_exec_mode() -> String { "stub".to_string() }
fn default_ws_url()   -> String { "wss://api.coin.z.com/ws/public/v1".to_string() }
fn default_raw_dir()  -> String { "./data/raw".to_string() }

// ── PublishEvent (mirrors connector crate) ────────────────────────────────────

pub enum PublishEvent {
    BatchStart(u64),
    BatchEnd(u64),
    LiveEvent(LiveEvent),
    RegisterInstrument {
        id:        u64,
        symbol:    String,
        tick_size: f64,
        lot_size:  f64,
    },
}

// ── GetOrders ─────────────────────────────────────────────────────────────────

pub trait GetOrders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order>;
}

// ── Simulated bar builder ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Bar {
    ts_ns:  i64,
    open:   f64,
    high:   f64,
    low:    f64,
    close:  f64,
}

struct BarBuilder {
    prices: Vec<f64>,
    last_close: Option<f64>,
}

impl BarBuilder {
    fn new() -> Self {
        Self { prices: Vec::new(), last_close: None }
    }

    fn push_trade(&mut self, price: f64) {
        self.prices.push(price);
    }

    fn close(&mut self, ts_ns: i64) -> Option<Bar> {
        if self.prices.is_empty() {
            let c = self.last_close?;
            return Some(Bar { ts_ns, open: c, high: c, low: c, close: c });
        }
        let open  = self.prices[0];
        let close = *self.prices.last().unwrap();
        let high  = self.prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let low   = self.prices.iter().cloned().fold(f64::INFINITY, f64::min);
        self.last_close = Some(close);
        self.prices.clear();
        Some(Bar { ts_ns, open, high, low, close })
    }
}

// ── Stub order manager ────────────────────────────────────────────────────────

struct StubOrder {
    order:  Order,
    symbol: String,
}

pub struct OrderManager {
    orders:   HashMap<u64, StubOrder>,
    next_gmo: AtomicU64,
}

impl OrderManager {
    fn new() -> Self {
        Self {
            orders:   HashMap::new(),
            next_gmo: AtomicU64::new(1),
        }
    }

    fn register(&mut self, symbol: String, order: Order) {
        self.orders.insert(order.order_id, StubOrder { order, symbol });
    }

    fn remove(&mut self, order_id: u64) -> Option<StubOrder> {
        self.orders.remove(&order_id)
    }

    fn resting_orders(&self) -> Vec<(String, Order)> {
        self.orders
            .values()
            .map(|s| (s.symbol.clone(), s.order.clone()))
            .collect()
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .values()
            .filter(|s| symbol.as_deref().map_or(true, |sym| s.symbol == sym))
            .map(|s| s.order.clone())
            .collect()
    }
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

// ── Raw recorder ─────────────────────────────────────────────────────────────

struct RawRecorder {
    raw_dir:    PathBuf,
    writers:    HashMap<String, GzEncoder<fs::File>>,
    current_date: String,
}

impl RawRecorder {
    fn new(raw_dir: &str) -> Self {
        Self {
            raw_dir: PathBuf::from(raw_dir),
            writers: HashMap::new(),
            current_date: String::new(),
        }
    }

    fn write(&mut self, channel: &str, recv_ts_ns: i64, raw: &str) -> std::io::Result<()> {
        let date = Utc::now().format("%Y-%m-%d").to_string();

        // Daily rotation: recreate writers if the date changed
        if date != self.current_date {
            self.writers.clear();
            self.current_date = date.clone();
        }

        if !self.writers.contains_key(channel) {
            let dir = self.raw_dir.join(&date);
            fs::create_dir_all(&dir)?;
            let path = dir.join(format!("{channel}.jsonl.gz"));
            let file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
            let gz = GzEncoder::new(file, Compression::default());
            self.writers.insert(channel.to_string(), gz);
        }

        let writer = self.writers.get_mut(channel).unwrap();
        writeln!(writer, "{recv_ts_ns}\t{raw}")?;
        Ok(())
    }
}

// ── WS message structs ────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct GmoMessage {
    channel: String,
    #[serde(flatten)]
    rest: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct OrderbookLevel {
    price: String,
    size:  String,
}

#[derive(Deserialize, Debug)]
struct OrderbookMsg {
    bids: Vec<OrderbookLevel>,
    asks: Vec<OrderbookLevel>,
    timestamp: Option<String>,
}

#[derive(Deserialize, Debug)]
struct TradeMsg {
    price:    String,
    size:     String,
    side:     String,
    timestamp: Option<String>,
}

// ── Public WS stream ──────────────────────────────────────────────────────────

async fn run_public_stream(
    ws_url:        String,
    symbols:       Vec<String>,
    ev_tx:         UnboundedSender<PublishEvent>,
    raw_recorder:  bool,
    raw_dir:       String,
    order_manager: SharedOrderManager,
    bar_interval:  u64,   // seconds
) {
    let mut backoff_secs: u64 = 1;
    let mut recorder = if raw_recorder {
        Some(RawRecorder::new(&raw_dir))
    } else {
        None
    };

    loop {
        info!("Connecting to GMO public WS: {ws_url}");
        match connect_inner(
            &ws_url,
            &symbols,
            &ev_tx,
            recorder.as_mut(),
            &order_manager,
            bar_interval,
        )
        .await
        {
            Ok(()) => {
                info!("GMO WS stream ended cleanly; reconnecting.");
            }
            Err(e) => {
                error!("GMO WS error: {e}; reconnecting in {backoff_secs}s");
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Error(
                        LiveError::with(ErrorKind::ConnectionInterrupted, e.into()),
                    )))
                    .unwrap_or(());
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
                continue;
            }
        }
        backoff_secs = 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn connect_inner(
    ws_url:        &str,
    symbols:       &[String],
    ev_tx:         &UnboundedSender<PublishEvent>,
    recorder:      Option<&mut RawRecorder>,
    order_manager: &SharedOrderManager,
    bar_interval:  u64,
) -> Result<(), GmoError> {
    let request = ws_url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();

    // Subscribe with 1-second gap between subscribes (GMO rate limit)
    for symbol in symbols {
        let sub = serde_json::json!({
            "command": "subscribe",
            "channel": "orderbooks",
            "symbol": symbol,
        });
        write
            .send(Message::Text(sub.to_string().into()))
            .await?;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let sub = serde_json::json!({
            "command": "subscribe",
            "channel": "trades",
            "symbol": symbol,
        });
        write
            .send(Message::Text(sub.to_string().into()))
            .await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    info!("GMO WS subscribed to {:?}", symbols);

    // Bar builder per symbol
    let mut bar_builders: HashMap<String, BarBuilder> = HashMap::new();
    for sym in symbols {
        bar_builders.insert(sym.clone(), BarBuilder::new());
    }

    // Bar timer: next aligned boundary
    let bar_secs = bar_interval;
    let now_secs = Utc::now().timestamp() as u64;
    let next_bar = ((now_secs / bar_secs) + 1) * bar_secs;
    let wait_ms  = (next_bar * 1000).saturating_sub(Utc::now().timestamp_millis() as u64);
    let mut bar_timer = time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(wait_ms),
        Duration::from_secs(bar_secs),
    );

    // We hold the recorder in a local var; ownership juggling via Option
    let mut rec = recorder;

    loop {
        select! {
            _ = bar_timer.tick() => {
                let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
                let resting = order_manager.lock().unwrap().resting_orders();

                for (symbol, builder) in bar_builders.iter_mut() {
                    if let Some(bar) = builder.close(ts_ns) {
                        // Simulate fills against the closed bar
                        let mut fills: Vec<(String, Order)> = Vec::new();
                        for (sym, order) in &resting {
                            if sym != symbol {
                                continue;
                            }
                            let p = order.price_tick as f64 * order.tick_size;
                            if order.side == Side::Buy && bar.low < p {
                                let fill_px = p.min(bar.open);
                                let mut filled = order.clone();
                                filled.status = Status::Filled;
                                filled.exec_price_tick = (fill_px / filled.tick_size).round() as i64;
                                filled.exec_qty = filled.qty;
                                filled.exch_timestamp = ts_ns;
                                fills.push((sym.clone(), filled));
                            } else if order.side == Side::Sell && bar.high > p {
                                let fill_px = p.max(bar.open);
                                let mut filled = order.clone();
                                filled.status = Status::Filled;
                                filled.exec_price_tick = (fill_px / filled.tick_size).round() as i64;
                                filled.exec_qty = filled.qty;
                                filled.exch_timestamp = ts_ns;
                                fills.push((sym.clone(), filled));
                            }
                        }

                        // Remove filled orders from manager and publish
                        for (sym, order) in fills {
                            order_manager.lock().unwrap().remove(order.order_id);
                            ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol: sym,
                                    order,
                                }))
                                .unwrap_or(());
                        }
                    }
                }
            }

            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let recv_ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
                    let raw = text.as_str();

                    // Parse channel to decide which recorder file to use
                    let channel_key: Option<String> = serde_json::from_str::<serde_json::Value>(raw)
                        .ok()
                        .and_then(|v| v.get("channel").and_then(|c| c.as_str()).map(|s| s.to_string()));

                    // Record raw frame
                    if let (Some(rec_ref), Some(chan)) = (rec.as_mut(), channel_key.as_deref()) {
                        if let Err(e) = rec_ref.write(chan, recv_ts_ns, raw) {
                            warn!("raw recorder write error: {e}");
                        }
                    }

                    if let Err(e) = handle_message(raw, recv_ts_ns, ev_tx, &mut bar_builders) {
                        error!("parse error: {e}");
                    }
                }
                Some(Ok(Message::Ping(data))) => {
                    write.send(Message::Pong(data)).await?;
                }
                Some(Ok(Message::Close(f))) => {
                    return Err(GmoError::ConnectionAbort(
                        f.map(|f| f.to_string()).unwrap_or_default(),
                    ));
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(GmoError::from(e)),
                None => return Err(GmoError::ConnectionInterrupted),
            }
        }
    }
}

fn handle_message(
    raw:           &str,
    recv_ts_ns:    i64,
    ev_tx:         &UnboundedSender<PublishEvent>,
    bar_builders:  &mut HashMap<String, BarBuilder>,
) -> Result<(), GmoError> {
    let msg: GmoMessage = serde_json::from_str(raw)?;
    let local_ts = recv_ts_ns;

    match msg.channel.as_str() {
        "orderbooks" => {
            let ob: OrderbookMsg = serde_json::from_value(msg.rest)?;
            let exch_ts = ob.timestamp
                .as_deref()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|ms| ms * 1_000_000)
                .unwrap_or(local_ts);

            // Determine symbol from original JSON (re-parse to get symbol field)
            let raw_val: serde_json::Value = serde_json::from_str(raw)?;
            let symbol = raw_val
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("BTC_JPY")
                .to_string();

            // Send clear + bid snapshot + ask snapshot as a batch
            ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap_or(());

            ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: symbol.clone(),
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
                .unwrap_or(());

            for level in &ob.bids {
                let px: f64 = level.price.parse().unwrap_or(0.0);
                let qty: f64 = level.size.parse().unwrap_or(0.0);
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                        symbol: symbol.clone(),
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
                    .unwrap_or(());
            }

            for level in &ob.asks {
                let px: f64 = level.price.parse().unwrap_or(0.0);
                let qty: f64 = level.size.parse().unwrap_or(0.0);
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                        symbol: symbol.clone(),
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
                    .unwrap_or(());
            }

            ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap_or(());
        }

        "trades" => {
            let trade: TradeMsg = serde_json::from_value(msg.rest)?;
            let exch_ts = trade.timestamp
                .as_deref()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|ms| ms * 1_000_000)
                .unwrap_or(local_ts);

            let raw_val: serde_json::Value = serde_json::from_str(raw)?;
            let symbol = raw_val
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("BTC_JPY")
                .to_string();

            let px:  f64 = trade.price.parse().unwrap_or(0.0);
            let qty: f64 = trade.size.parse().unwrap_or(0.0);

            let is_buy = trade.side.to_uppercase() == "BUY";
            let ev_flag = if is_buy {
                LOCAL_BUY_TRADE_EVENT
            } else {
                LOCAL_SELL_TRADE_EVENT
            };

            // Update bar builder
            if let Some(builder) = bar_builders.get_mut(&symbol) {
                builder.push_trade(px);
            }

            ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol,
                    event: Event {
                        ev: ev_flag,
                        exch_ts,
                        local_ts,
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap_or(());
        }

        other => {
            warn!("Unknown GMO channel: {other}");
        }
    }

    Ok(())
}

// ── Position tracker ──────────────────────────────────────────────────────────

struct Position {
    qty:     f64,
    exch_ts: i64,
}

// ── Publish task ──────────────────────────────────────────────────────────────

async fn run_publish_task(
    name:           &str,
    order_manager:  Arc<Mutex<dyn GetOrders + Send + 'static>>,
    mut rx:         UnboundedReceiver<PublishEvent>,
    shutdown:       Arc<Notify>,
) -> Result<(), ChannelError> {
    let mut depth: HashMap<String, FusedHashMapMarketDepth> = HashMap::new();
    let mut position: HashMap<String, Position> = HashMap::new();
    let bot_tx = IceoryxBuilder::new(name).bot(false).sender()?;

    loop {
        select! {
            _ = shutdown.notified() => break,
            Some(msg) = rx.recv() => {
                match msg {
                    PublishEvent::RegisterInstrument { id, symbol, tick_size, lot_size } => {
                        bot_tx.send(id, &LiveEvent::BatchStart)?;

                        for order in order_manager.lock().unwrap().orders(Some(symbol.clone())) {
                            bot_tx.send(id, &LiveEvent::Order {
                                symbol: symbol.clone(),
                                order,
                            })?;
                        }

                        if let Some(pos) = position.get(&symbol) {
                            bot_tx.send(id, &LiveEvent::Position {
                                symbol: symbol.clone(),
                                qty: pos.qty,
                                exch_ts: pos.exch_ts,
                            })?;
                        }

                        match depth.entry(symbol) {
                            Entry::Occupied(mut entry) => {
                                for event in entry.get_mut().snapshot() {
                                    bot_tx.send(id, &LiveEvent::Feed {
                                        symbol: entry.key().clone(),
                                        event,
                                    })?;
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(FusedHashMapMarketDepth::new(tick_size, lot_size));
                            }
                        }

                        bot_tx.send(id, &LiveEvent::BatchEnd)?;
                    }
                    PublishEvent::LiveEvent(ev) => {
                        for out_ev in handle_ev(ev, &mut depth, &mut position) {
                            bot_tx.send(TO_ALL, &out_ev)?;
                        }
                    }
                    PublishEvent::BatchStart(id) => {
                        bot_tx.send(id, &LiveEvent::BatchStart)?;
                    }
                    PublishEvent::BatchEnd(id) => {
                        bot_tx.send(id, &LiveEvent::BatchEnd)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn handle_ev(
    ev:       LiveEvent,
    depth:    &mut HashMap<String, FusedHashMapMarketDepth>,
    position: &mut HashMap<String, Position>,
) -> Vec<LiveEvent> {
    match &ev {
        LiveEvent::Feed { symbol, event } => {
            if event.is(BUY_EVENT | DEPTH_EVENT) {
                if let Some(d) = depth.get_mut(symbol) {
                    return d
                        .update_bid_depth(event.clone())
                        .iter()
                        .map(|e| LiveEvent::Feed { symbol: symbol.clone(), event: e.clone() })
                        .collect();
                }
                return vec![];
            } else if event.is(SELL_EVENT | DEPTH_EVENT) {
                if let Some(d) = depth.get_mut(symbol) {
                    return d
                        .update_ask_depth(event.clone())
                        .iter()
                        .map(|e| LiveEvent::Feed { symbol: symbol.clone(), event: e.clone() })
                        .collect();
                }
                return vec![];
            } else if event.is(DEPTH_CLEAR_EVENT) {
                if let Some(d) = depth.get_mut(symbol) {
                    d.clear_depth(Side::None, 0.0, 0);
                }
            }
        }
        LiveEvent::Position { symbol, qty, exch_ts } => {
            if let Some(pos) = position.get_mut(symbol) {
                if *exch_ts >= pos.exch_ts {
                    pos.qty = *qty;
                    return vec![ev];
                }
                return vec![];
            } else {
                position.insert(symbol.clone(), Position { qty: *qty, exch_ts: *exch_ts });
                return vec![ev];
            }
        }
        _ => {}
    }
    vec![ev]
}

// ── Receive task (bot → connector orders) ─────────────────────────────────────

fn run_receive_task(
    name:          &str,
    tx:            UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    instruments:   &[InstrumentConfig],
) -> Result<(), ChannelError> {
    let node = NodeBuilder::new()
        .signal_handling_mode(SignalHandlingMode::Disabled)
        .create::<ipc::Service>()
        .map_err(|e| ChannelError::BuildError(e.to_string()))?;
    let bot_rx = IceoryxBuilder::new(name).bot(false).receiver()?;

    loop {
        match node.wait(Duration::from_nanos(1000)) {
            Ok(()) => {
                while let Some((id, ev)) = bot_rx.receive()? {
                    match ev {
                        LiveRequest::Order { symbol, order } => match order.req {
                            Status::New => {
                                // Stub: register order in manager and ACK immediately
                                let mut ack = order.clone();
                                ack.status = Status::New;
                                ack.req    = Status::None;
                                order_manager
                                    .lock()
                                    .unwrap()
                                    .register(symbol.clone(), order);
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order: ack,
                                }))
                                .unwrap_or(());
                            }
                            Status::Canceled => {
                                if let Some(stub) =
                                    order_manager.lock().unwrap().remove(order.order_id)
                                {
                                    let mut cancelled = stub.order;
                                    cancelled.status = Status::Canceled;
                                    cancelled.req    = Status::None;
                                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                        symbol,
                                        order: cancelled,
                                    }))
                                    .unwrap_or(());
                                }
                            }
                            _ => {}
                        },
                        LiveRequest::RegisterInstrument { symbol, tick_size, lot_size } => {
                            tx.send(PublishEvent::RegisterInstrument {
                                id,
                                symbol: symbol.clone(),
                                tick_size,
                                lot_size,
                            })
                            .unwrap_or(());
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

// ── Args ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of this connector instance (used as Iceoryx2 service name).
    name: String,

    /// Connector type (must be "gmocoin").
    connector: String,

    /// Path to TOML config file.
    config: String,
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Terminate on any child-thread panic
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        orig_hook(info);
        exit(1);
    }));

    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let config_str = read_to_string(&args.config)
        .unwrap_or_else(|e| {
            error!(?e, config = args.config, "Could not read config file.");
            exit(1);
        });

    let config: Config = toml::from_str(&config_str)
        .unwrap_or_else(|e| {
            error!(?e, "Could not parse config file.");
            exit(1);
        });

    info!(?config, "GMO Coin connector starting (exec_mode={})", config.exec_mode);

    let order_manager: SharedOrderManager = Arc::new(Mutex::new(OrderManager::new()));
    let (pub_tx, pub_rx) = unbounded_channel::<PublishEvent>();

    let shutdown = Arc::new(Notify::new());
    let shutdown2 = shutdown.clone();

    // Shutdown listener
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
            select! {
                _ = signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = signal::ctrl_c().await;
        }
        shutdown2.notify_waiters();
    });

    // Symbols from config
    let symbols: Vec<String> = config
        .instruments
        .iter()
        .map(|i| i.symbol.clone())
        .collect();

    // Spawn public WS stream task
    let ws_url       = config.public_stream_url.clone();
    let raw_recorder = config.raw_recorder;
    let raw_dir      = config.raw_dir.clone();
    let om2          = order_manager.clone();
    let tx2          = pub_tx.clone();
    let syms2        = symbols.clone();
    tokio::spawn(async move {
        run_public_stream(ws_url, syms2, tx2, raw_recorder, raw_dir, om2, 5).await;
    });

    // Publish task on a dedicated thread
    let name          = args.name.clone();
    let om_trait: Arc<Mutex<dyn GetOrders + Send + 'static>> = order_manager.clone();
    let shutdown3     = shutdown.clone();
    let handle = thread::spawn(move || {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            run_publish_task(&name, om_trait, pub_rx, shutdown3)
                .await
                .unwrap_or_else(|e| error!(?e, "publish task error"));
        });
    });

    // Receive task (blocks current thread)
    let instruments = config.instruments.clone();
    run_receive_task(&args.name, pub_tx, order_manager, &instruments)
        .unwrap_or_else(|e| error!(?e, "receive task error"));

    let _ = handle.join();
}
