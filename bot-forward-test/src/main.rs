//! GMO Coin BTC_JPY forward-test bot skeleton.
//!
//! Uses `LiveBot` from hftbacktest to receive market data events from the
//! connector via Iceoryx2 shared memory. On each 5-second bar close:
//!   - Logs bar summary (trade count, OHLCV placeholder)
//!   - Placeholder for model inference
//!   - Placeholder for order submission
//!
//! Usage:
//!   cargo run --release -p bot-forward-test -- --data-dir ./data

use std::io::{self, Write};

use clap::Parser;
use hftbacktest::{
    live::{Instrument, LiveBot, LiveBotBuilder, ipc::iceoryx::IceoryxUnifiedChannel},
    prelude::*,
    types::{ElapseResult, ErrorKind, LiveError},
};
use serde::Deserialize;
use tracing::{error, info, warn};

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct InstrumentConfig {
    connector: String,
    symbol:    String,
    tick_size: f64,
    lot_size:  f64,
}

#[derive(Deserialize, Debug)]
struct Config {
    #[serde(default = "default_bar_sec")]
    bar_sec: u64,
    #[serde(default)]
    instruments: Vec<InstrumentConfig>,
}

fn default_bar_sec() -> u64 { 5 }

impl Default for Config {
    fn default() -> Self {
        Self {
            bar_sec: 5,
            instruments: vec![InstrumentConfig {
                connector: "gmocoin".to_string(),
                symbol:    "BTC_JPY".to_string(),
                tick_size: 1.0,
                lot_size:  0.01,
            }],
        }
    }
}

// ── Bar accumulator ───────────────────────────────────────────────────────────

struct BarAcc {
    open:        Option<f64>,
    high:        f64,
    low:         f64,
    close:       f64,
    volume:      f64,
    trade_count: usize,
    bar_sec:     u64,
    next_bar_ns: i64,
}

impl BarAcc {
    fn new(bar_sec: u64) -> Self {
        let now_ns  = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let bar_ns  = (bar_sec * 1_000_000_000) as i64;
        let next    = ((now_ns / bar_ns) + 1) * bar_ns;
        Self {
            open:        None,
            high:        f64::NEG_INFINITY,
            low:         f64::INFINITY,
            close:       0.0,
            volume:      0.0,
            trade_count: 0,
            bar_sec,
            next_bar_ns: next,
        }
    }

    /// Push a trade into the accumulator.
    fn push(&mut self, price: f64, qty: f64) {
        if self.open.is_none() {
            self.open = Some(price);
        }
        if price > self.high { self.high = price; }
        if price < self.low  { self.low  = price; }
        self.close       = price;
        self.volume     += qty;
        self.trade_count += 1;
    }

    /// Return true if a bar should be closed at the given nanosecond timestamp.
    fn should_close(&self, now_ns: i64) -> bool {
        now_ns >= self.next_bar_ns
    }

    /// Close the bar: returns (bar_ts_ns, open, high, low, close, volume, trade_count).
    fn close_bar(&mut self) -> (i64, f64, f64, f64, f64, f64, usize) {
        let bar_ns    = (self.bar_sec * 1_000_000_000) as i64;
        let bar_ts_ns = self.next_bar_ns;
        self.next_bar_ns += bar_ns;

        let open  = self.open.unwrap_or(self.close);
        let high  = if self.trade_count == 0 { self.close } else { self.high };
        let low   = if self.trade_count == 0 { self.close } else { self.low  };
        let vol   = self.volume;
        let cnt   = self.trade_count;
        let close = self.close;

        // Reset
        self.open        = None;
        self.high        = f64::NEG_INFINITY;
        self.low         = f64::INFINITY;
        self.volume      = 0.0;
        self.trade_count = 0;

        (bar_ts_ns, open, high, low, close, vol, cnt)
    }
}

// ── Args ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Data directory for output files.
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// Path to TOML config file. Defaults use built-in config.
    #[arg(long, default_value = "config/bot.toml")]
    config: String,
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Load config (graceful fallback to defaults if file not found)
    let config: Config = std::fs::read_to_string(&args.config)
        .ok()
        .and_then(|s| toml::from_str(&s).ok())
        .unwrap_or_else(|| {
            warn!(
                config = args.config,
                "Config file not found or parse error; using built-in defaults."
            );
            Config::default()
        });

    info!(?config, "bot-forward-test starting");

    // Build LiveBot
    let instruments: Vec<_> = config
        .instruments
        .iter()
        .map(|inst| {
            Instrument::new(
                &inst.connector,
                &inst.symbol,
                inst.tick_size,
                inst.lot_size,
                HashMapMarketDepth::new(inst.tick_size, inst.lot_size),
                64,  // last_trades_capacity
            )
        })
        .collect();

    if instruments.is_empty() {
        error!("No instruments configured; exiting.");
        return;
    }

    let mut builder: LiveBotBuilder<HashMapMarketDepth> = LiveBotBuilder::new()
        .error_handler(|err: LiveError| {
            match err.kind {
                ErrorKind::ConnectionInterrupted => {
                    warn!("ConnectionInterrupted — waiting for reconnect.");
                    Ok(())
                }
                _ => {
                    error!(?err, "Unhandled live error.");
                    Ok(())
                }
            }
        });

    for inst in instruments {
        builder = builder.register(inst);
    }

    let mut hbt: LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> =
        match builder.build::<IceoryxUnifiedChannel>() {
            Ok(b) => b,
            Err(e) => {
                error!(?e, "Failed to build LiveBot.");
                return;
            }
        };

    info!("LiveBot built; entering event loop (bar_sec={})", config.bar_sec);

    // Bar accumulators per asset
    let n_assets = hbt.num_assets();
    let mut bar_accs: Vec<BarAcc> = (0..n_assets)
        .map(|_| BarAcc::new(config.bar_sec))
        .collect();

    // Event loop
    let mut bar_count: u64 = 0;
    loop {
        // Elapse up to 100ms; process whatever arrives
        match hbt.elapse(100_000_000) {
            Ok(ElapseResult::Ok) | Ok(ElapseResult::MarketFeed) | Ok(ElapseResult::OrderResponse) => {}
            Ok(ElapseResult::EndOfData) => {
                info!("End of data signal received; shutting down.");
                break;
            }
            Err(e) => {
                error!(?e, "LiveBot::elapse error; shutting down.");
                break;
            }
        }

        // Drain recent trades into bar accumulators
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        for asset_no in 0..n_assets {
            // Collect trades since last call
            for trade_event in hbt.last_trades(asset_no) {
                let price = trade_event.px;
                let qty   = trade_event.qty;
                bar_accs[asset_no].push(price, qty);
            }

            // Check if bar should close
            if bar_accs[asset_no].should_close(now_ns) {
                let (bar_ts_ns, open, high, low, close, volume, trade_count) =
                    bar_accs[asset_no].close_bar();

                bar_count += 1;

                // ── Heartbeat ─────────────────────────────────────────────────
                let ts_str = chrono::DateTime::from_timestamp_nanos(bar_ts_ns)
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string();
                println!(
                    "[bar #{bar_count}] ts={ts_str}  O={open:.0} H={high:.0} L={low:.0} C={close:.0} \
                     vol={volume:.4} trades={trade_count}"
                );
                let _ = io::stdout().flush();

                // ── Placeholder: feature computation ─────────────────────────
                info!(
                    bar = bar_count,
                    trade_count,
                    "bar closed, {trade_count} trades"
                );

                // ── Placeholder: model inference ──────────────────────────────
                let pred_buy:  f64 = 0.5;
                let pred_sell: f64 = 0.5;
                info!(pred_buy, pred_sell, "placeholder inference");
                println!("[bar #{bar_count}] pred_buy={pred_buy:.4} pred_sell={pred_sell:.4}");
                let _ = io::stdout().flush();

                // ── Placeholder: order submission ─────────────────────────────
                // In Phase-2 this would call hbt.submit_buy_limit_order / sell.
                // For Phase-1 (sim) the connector handles fills; we just log.
                info!("would submit orders (buy={pred_buy:.4} sell={pred_sell:.4})");
                println!("[bar #{bar_count}] would submit orders (signals: buy={pred_buy} sell={pred_sell})");
                let _ = io::stdout().flush();
            }
        }
    }

    info!("bot-forward-test finished (bars_closed={bar_count})");
}
