"""
Binance Spot BTC/USDT 2026-03-24 simple market making simulation.
Strategy: every 5 seconds, place buy at mid-3bps and sell at mid+3bps.
"""
import os
import sys

# Use installed package, not the source directory
sys.path = [p for p in sys.path if 'hftbacktest' not in p]

import gzip
import json

import numpy as np
from numba import njit

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, LIMIT, GTC
from hftbacktest.data.validation import correct_event_order, correct_local_timestamp, validate_event_order
from hftbacktest.types import (
    DEPTH_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    TRADE_EVENT,
    BUY_EVENT,
    SELL_EVENT,
    event_dtype,
)

DATA_PATH = '/mnt/external/fromThinkpad/hftbacktest/data/spot/btcusdt_20260324.gz'
NPZ_PATH  = '/mnt/external/fromThinkpad/hftbacktest/data/spot/btcusdt_20260324.npz'


# ---------------------------------------------------------------------------
# Spot data converter (adapted from binancefutures.convert)
# Differences from futures:
#   - depthUpdate: uses 'E' (event time ms) instead of 'T' (transaction time ms)
#   - bookTicker:  no 'e' field, no timestamp -> use local_timestamp as proxy
#   - trade:       uses 'T' field (same as futures), no 'X' filter
#   - snapshot:    not present in collected files; depth builds from updates only
# ---------------------------------------------------------------------------
def convert_spot(input_filename, output_filename=None, base_latency=0, buffer_size=200_000_000):
    timestamp_slice = 19   # nanosecond local timestamp (19 digits)
    ms_to_ns = 1_000_000   # exchange timestamps are in milliseconds

    tmp = np.empty(buffer_size, event_dtype)
    row_num = 0

    with gzip.open(input_filename, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            local_timestamp = int(line[:timestamp_slice])
            message = json.loads(line[timestamp_slice + 1:])
            data = message.get('data')

            if data is None:
                # Top-level snapshot: {"lastUpdateId":..., "bids":[...], "asks":[...]}
                # (rare; use local_timestamp as exchange time)
                bids = message.get('bids', [])
                asks = message.get('asks', [])
                exch_timestamp = local_timestamp
                if bids:
                    tmp[row_num] = (DEPTH_CLEAR_EVENT | BUY_EVENT, exch_timestamp, local_timestamp,
                                    float(bids[-1][0]), 0, 0, 0, 0)
                    row_num += 1
                    for px, qty in bids:
                        tmp[row_num] = (DEPTH_SNAPSHOT_EVENT | BUY_EVENT, exch_timestamp, local_timestamp,
                                        float(px), float(qty), 0, 0, 0)
                        row_num += 1
                if asks:
                    tmp[row_num] = (DEPTH_CLEAR_EVENT | SELL_EVENT, exch_timestamp, local_timestamp,
                                    float(asks[-1][0]), 0, 0, 0, 0)
                    row_num += 1
                    for px, qty in asks:
                        tmp[row_num] = (DEPTH_SNAPSHOT_EVENT | SELL_EVENT, exch_timestamp, local_timestamp,
                                        float(px), float(qty), 0, 0, 0)
                        row_num += 1
                continue

            evt = data.get('e')

            if evt == 'depthUpdate':
                # Spot depthUpdate has 'E' but not 'T'
                exch_timestamp = int(data['E']) * ms_to_ns
                for px, qty in data.get('b', []):
                    tmp[row_num] = (DEPTH_EVENT | BUY_EVENT, exch_timestamp, local_timestamp,
                                    float(px), float(qty), 0, 0, 0)
                    row_num += 1
                for px, qty in data.get('a', []):
                    tmp[row_num] = (DEPTH_EVENT | SELL_EVENT, exch_timestamp, local_timestamp,
                                    float(px), float(qty), 0, 0, 0)
                    row_num += 1

            elif evt == 'trade':
                # Spot trade has 'T' (transaction time ms)
                exch_timestamp = int(data['T']) * ms_to_ns
                side = SELL_EVENT if data['m'] else BUY_EVENT  # 'm': true = sell taker
                tmp[row_num] = (TRADE_EVENT | side, exch_timestamp, local_timestamp,
                                float(data['p']), float(data['q']), 0, 0, 0)
                row_num += 1

            elif evt is None:
                # bookTicker: {"u":..., "s":..., "b":..., "B":..., "a":..., "A":...}
                # No exchange timestamp; use local_timestamp as proxy
                # Store as custom events 103 (best bid) and 104 (best ask)
                exch_timestamp = local_timestamp
                tmp[row_num] = (103, exch_timestamp, local_timestamp,
                                float(data['b']), float(data['B']), 0, 0, 0)
                row_num += 1
                tmp[row_num] = (104, exch_timestamp, local_timestamp,
                                float(data['a']), float(data['A']), 0, 0, 0)
                row_num += 1

    tmp = tmp[:row_num]
    print(f'Parsed {row_num:,} events')

    print('Correcting local timestamp...')
    tmp = correct_local_timestamp(tmp, base_latency)

    print('Correcting event order...')
    data_out = correct_event_order(
        tmp,
        np.argsort(tmp['exch_ts'], kind='mergesort'),
        np.argsort(tmp['local_ts'], kind='mergesort'),
    )

    validate_event_order(data_out)

    if output_filename is not None:
        print(f'Saving to {output_filename}')
        np.savez_compressed(output_filename, data=data_out)

    return data_out


# ---------------------------------------------------------------------------
# Market making strategy
# ---------------------------------------------------------------------------
SPREAD_BPS = 3.0   # half-spread in basis points (3bps each side)
INTERVAL_NS = 5 * 1_000_000_000  # 5 seconds in nanoseconds
ORDER_QTY   = 0.001               # order size in BTC

@njit
def run_mm(hbt):
    """Simple market maker: refresh bid/ask at mid ± 3bps every 5 seconds."""
    buy_order_id  = 1
    sell_order_id = 2
    spread_mult   = SPREAD_BPS / 10_000.0  # bps -> fraction

    buy_active  = False
    sell_active = False

    while hbt.elapse(INTERVAL_NS) == 0:
        depth = hbt.depth(0)
        best_bid = depth.best_bid
        best_ask = depth.best_ask

        # Skip if the spread is invalid (book not yet populated)
        if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
            continue

        mid = (best_bid + best_ask) * 0.5
        tick = depth.tick_size

        buy_px  = round(mid * (1.0 - spread_mult) / tick) * tick
        sell_px = round(mid * (1.0 + spread_mult) / tick) * tick

        # Cancel previous orders before re-quoting
        if buy_active:
            hbt.cancel(0, buy_order_id, False)
            buy_active = False
        if sell_active:
            hbt.cancel(0, sell_order_id, False)
            sell_active = False

        # Flush cancels
        hbt.elapse(0)

        # Place new orders
        hbt.submit_buy_order(0, buy_order_id,  buy_px,  ORDER_QTY, GTC, LIMIT, False)
        hbt.submit_sell_order(0, sell_order_id, sell_px, ORDER_QTY, GTC, LIMIT, False)
        buy_active  = True
        sell_active = True

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    # Step 1: convert data (skip if npz already exists)
    if not os.path.exists(NPZ_PATH):
        print('Converting spot data...')
        convert_spot(DATA_PATH, output_filename=NPZ_PATH)
    else:
        print(f'Using cached {NPZ_PATH}')

    # Step 2: build backtest
    # BTC/USDT spot parameters
    TICK_SIZE = 0.01     # minimum price increment
    LOT_SIZE  = 0.00001  # minimum quantity increment
    # Binance Spot fees: 0.1% maker & taker (BNB discount not modelled)
    MAKER_FEE = 0.001
    TAKER_FEE = 0.001
    # Latency: assume 10ms round-trip (10_000_000 ns)
    LATENCY_NS = 10_000_000

    asset = (
        BacktestAsset()
        .data([NPZ_PATH])
        .linear_asset(1.0)
        .constant_order_latency(LATENCY_NS, LATENCY_NS)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(MAKER_FEE, TAKER_FEE)
        .tick_size(TICK_SIZE)
        .lot_size(LOT_SIZE)
    )

    hbt = HashMapMarketDepthBacktest([asset])

    print('Running market making simulation...')
    run_mm(hbt)

    # Step 3: results
    state = hbt.state_values(0)
    print()
    print('=== Results ===')
    print(f'  Balance (cash PnL): {state.balance:+.4f} USDT')
    print(f'  Position          : {state.position:.6f} BTC')
    print(f'  Fee paid          : {state.fee:.4f} USDT')
    print(f'  Num trades        : {state.num_trades}')
    print(f'  Trading volume    : {state.trading_volume:.4f} USDT')

    hbt.close()
