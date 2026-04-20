use anyhow::anyhow;
use clap::Parser;
use tokio::{self, select, signal, sync::mpsc::unbounded_channel};
use tracing::{error, info};

use crate::file::Writer;

mod binance;
mod bitbank;
mod bitflyer;
mod bybit;
mod coinbase;
mod coincheck;
mod error;
mod file;
mod gmocoin;
mod gmofx;
mod hyperliquid;
mod throttler;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    exchange: String,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let (writer_tx, mut writer_rx) = unbounded_channel();

    let handle = match args.exchange.as_str() {
        "binance" | "binancespot" => {
            let streams = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@100ms"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binance::run_collection(streams, args.symbols, writer_tx))
        }
        "bybit" => {
            let topics = [
                "orderbook.1.$symbol",
                "orderbook.50.$symbol",
                "orderbook.500.$symbol",
                "publicTrade.$symbol",
            ]
            .iter()
            .map(|topic| topic.to_string())
            .collect();

            tokio::spawn(bybit::run_collection(topics, args.symbols, writer_tx))
        }
        "hyperliquid" => {
            let subscriptions = ["trades", "l2Book", "bbo"]
                .iter()
                .map(|sub| sub.to_string())
                .collect();

            tokio::spawn(hyperliquid::run_collection(
                subscriptions,
                args.symbols,
                writer_tx,
            ))
        }
        "bitflyer" => {
            let channel_templates = [
                "lightning_board_snapshot_$symbol",
                "lightning_board_$symbol",
                "lightning_executions_$symbol",
            ]
            .iter()
            .map(|ch| ch.to_string())
            .collect();

            tokio::spawn(bitflyer::run_collection(
                channel_templates,
                args.symbols,
                writer_tx,
            ))
        }
        "coinbase" => {
            let channels = ["market_trades", "level2"]
                .iter()
                .map(|ch| ch.to_string())
                .collect();

            tokio::spawn(coinbase::run_collection(channels, args.symbols, writer_tx))
        }
        "coincheck" => {
            let channel_templates = ["$symbol-trades", "$symbol-orderbook"]
                .iter()
                .map(|ch| ch.to_string())
                .collect();

            tokio::spawn(coincheck::run_collection(
                channel_templates,
                args.symbols,
                writer_tx,
            ))
        }
        "bitbank" => {
            let channel_templates = [
                "depth_whole_$symbol",
                "depth_diff_$symbol",
                "transactions_$symbol",
            ]
            .iter()
            .map(|ch| ch.to_string())
            .collect();

            tokio::spawn(bitbank::run_collection(
                channel_templates,
                args.symbols,
                writer_tx,
            ))
        }
        "gmocoin" => {
            let channels = ["orderbooks", "trades"]
                .iter()
                .map(|ch| ch.to_string())
                .collect();

            tokio::spawn(gmocoin::run_collection(channels, args.symbols, writer_tx))
        }
        "gmocoin_trades" => {
            let channels = ["trades"]
                .iter()
                .map(|ch| ch.to_string())
                .collect();

            tokio::spawn(gmocoin::run_collection(channels, args.symbols, writer_tx))
        }
        "gmofx" => {
            let channels = ["ticker"]
                .iter()
                .map(|ch| ch.to_string())
                .collect();

            tokio::spawn(gmofx::run_collection(channels, args.symbols, writer_tx))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

    let mut writer = Writer::new(&args.path);
    loop {
        select! {
            _ = signal::ctrl_c() => {
                info!("ctrl-c received");
                break;
            }
            r = writer_rx.recv() => match r {
                Some((recv_time, symbol, data)) => {
                    if let Err(error) = writer.write(recv_time, symbol, data) {
                        error!(?error, "write error");
                        break;
                    }
                }
                None => {
                    break;
                }
            }
        }
    }
    // let _ = handle.await;
    Ok(())
}
