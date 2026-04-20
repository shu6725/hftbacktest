#!/usr/bin/env bash
set -euo pipefail

# Output directory for collected data (override with DATA_DIR env var)
DATA_DIR="${DATA_DIR:-./data}"
mkdir -p "$DATA_DIR"

BINARY="./target/release/collector"

if [[ ! -x "$BINARY" ]]; then
    echo "Building collector..."
    cargo build --release
fi

echo "Starting collectors. Press Ctrl-C to stop."

# bitbank: btc_jpy eth_jpy link_jpy avax_jpy sui_jpy
"$BINARY" "$DATA_DIR" bitbank btc_jpy eth_jpy link_jpy avax_jpy sui_jpy &
PID_BITBANK=$!
echo "bitbank collector PID: $PID_BITBANK"

# Coinbase: linkusd avaxusd suiusd
"$BINARY" "$DATA_DIR" coinbase linkusd avaxusd suiusd &
PID_COINBASE=$!
echo "coinbase collector PID: $PID_COINBASE"

# GMO Coin — 1 process, 3 leverage pairs × 2 channels = 6 subs (within IP limit)
mkdir -p "$DATA_DIR/gmocoin"
"$BINARY" "$DATA_DIR/gmocoin" gmocoin BTC_JPY ETH_JPY XRP_JPY &
PID_GMOCOIN=$!

# GMO FX: USD_JPY
mkdir -p "$DATA_DIR/gmofx"
"$BINARY" "$DATA_DIR/gmofx" gmofx USD_JPY &
PID_GMOFX=$!
echo "gmofx collector PID: $PID_GMOFX"

# Terminate all on Ctrl-C
echo "gmocoin collector PID: $PID_GMOCOIN"

# Terminate all on Ctrl-C
trap 'echo "Stopping..."; kill $PID_BITBANK $PID_COINBASE $PID_GMOCOIN $PID_GMOFX 2>/dev/null; wait' INT TERM

wait $PID_BITBANK $PID_COINBASE $PID_GMOCOIN $PID_GMOFX
