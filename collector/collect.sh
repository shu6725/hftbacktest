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

# Terminate both on Ctrl-C
trap 'echo "Stopping..."; kill $PID_BITBANK $PID_COINBASE 2>/dev/null; wait' INT TERM

wait $PID_BITBANK $PID_COINBASE
