#!/usr/bin/env python3
"""
GMO BTC_JPY 5-second bar ML realtime pipeline.

Data sources:
  - GMO Coin BTC_JPY  : orderbooks + trades  (bar building + features)
  - GMO FX  USD_JPY   : ticker               (reference FX rate)
  - Coinbase BTC-USD  : level2               (arb reference)

Pipeline (every 5 seconds on bar close):
  1. Build 5s OHLCV bars from trades
  2. Compute all 37 features (identical logic to feature_monitor.py)
  3. Load LightGBM models (graceful fallback to 0.5 if unavailable)
  4. Run inference
  5. Generate buy/sell limit signals
  6. Simulated fill tracking
  7. Parquet output (bars + fills)
  8. Terminal display

Usage:
  LD_LIBRARY_PATH=$HOME/.local/lib python3 examples/ml_pipeline.py
"""

import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import websockets

try:
    import talib
    _TALIB = True
except ImportError:
    _TALIB = False
    print("[warn] ta-lib not available; TA-Lib features will be NaN", file=sys.stderr)

try:
    import lightgbm as lgb
    _LGB = True
except ImportError:
    _LGB = False
    print("[warn] lightgbm not available; predictions will default to 0.5", file=sys.stderr)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _ARROW = True
except ImportError:
    _ARROW = False
    print("[warn] pyarrow not available; Parquet output disabled", file=sys.stderr)

# ── Constants ────────────────────────────────────────────────────────────────
BAR_SEC    = 5         # bar interval in seconds
ROLL_N     = 32        # rolling window for microstructure features
BOOK_KEEP  = 10        # number of levels to keep from GMO orderbook
LIMIT_DIST = 500.0     # JPY distance from close for limit orders

DATA_DIR   = Path("data")

# ── Shared state ─────────────────────────────────────────────────────────────
_gmo_book = {"bids": [], "asks": [], "best_bid": None, "best_ask": None}
_usdjpy   = {"bid": None, "ask": None}
_cb_bids: dict[str, float] = {}
_cb_asks: dict[str, float] = {}
_btcusd   = {"bid": None, "ask": None}

# Per-bar trade accumulator
_acc_prices: list[float] = []
_acc_sizes:  list[float] = []
_acc_sides:  list[str]   = []   # "BUY" | "SELL"

# Closed bar history (newest last)
_bars: deque[dict] = deque(maxlen=128)

# Latest computed features and bar (written on bar close, read by display)
_last_bar:      Optional[dict] = None
_last_features: dict           = {}
_bar_count = 0

# ── Simulated trading state ───────────────────────────────────────────────────
_position:      float = 0.0    # BTC position (positive = long)
_realized_pnl:  float = 0.0
_active_buy_order:  Optional[dict] = None   # {price, size, bar_ts_ns}
_active_sell_order: Optional[dict] = None
_local_order_id: int = 0
_fills: list[dict] = []

# ── LightGBM models ───────────────────────────────────────────────────────────
_model_buy  = None
_model_sell = None

# ── Parquet output state ──────────────────────────────────────────────────────
_bar_rows:   list[dict] = []
_fill_rows:  list[dict] = []
_current_bar_hour: Optional[int] = None   # hour of current bar file
_current_bar_date: Optional[str] = None

# ── FEATURE_ORDER (identical to feature_monitor.py) ──────────────────────────
FEATURE_ORDER = [
    # Microstructure
    ("volume_z",                 4),
    ("spread_atr_rmean_32",      4),
    ("cum_volume_imb_rmean_32",  4),
    ("trade_cnt_rate_rmean_32",  4),
    ("trade_cnt_rate_rstd_32",   4),
    ("ofi_rmean_32",             4),
    ("ofi_rstd_32",              4),
    ("mid_diff1",                1),
    ("impact_spread_bps_diff1",  4),
    ("impact_spread_bps_pct1",   4),
    ("cum_volume_imb_diff1",     4),
    ("cum_volume_imb_pct1",      4),
    ("trade_cnt_rate_diff1",     4),
    # TA-Lib
    ("ADX_14",                   2),
    ("DX_14",                    2),
    ("CCI_14",                   2),
    ("RSI_14",                   2),
    ("WILLR_14",                 2),
    ("ULTOSC",                   2),
    ("MACD_macd",                6),
    ("MACD_macdhist",            6),
    ("STOCH_slowk",              2),
    ("STOCH_slowd",              2),
    ("STOCHF_fastk",             2),
    ("MFI_14",                   2),
    ("HT_DCPERIOD",              2),
    ("HT_DCPHASE",               2),
    ("HT_PHASOR_inphase",        4),
    ("HT_PHASOR_quadrature",     4),
    ("LINEARREG",                6),
    ("LINEARREG_ANGLE",          6),
    ("LINEARREG_INTERCEPT",      6),
    ("LINEARREG_SLOPE",          6),
    ("KAMA_30",                  6),
    ("MIDPOINT_14",              6),
    ("TEMA_30",                  6),
    ("BETA_5",                   4),
]

FEATURE_NAMES = [name for name, _ in FEATURE_ORDER]


# ── Startup: save feature_columns.json and load models ───────────────────────
def _init():
    global _model_buy, _model_sell

    # Save feature column list
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    feat_path = DATA_DIR / "feature_columns.json"
    with open(feat_path, "w") as f:
        json.dump(FEATURE_NAMES, f, indent=2)
    print(f"[init] feature_columns.json saved to {feat_path}")

    # Load LightGBM models
    if _LGB:
        buy_path  = DATA_DIR / "model_buy.lgb"
        sell_path = DATA_DIR / "model_sell.lgb"
        if buy_path.exists():
            _model_buy = lgb.Booster(model_file=str(buy_path))
            print(f"[init] loaded model_buy from {buy_path}")
        else:
            print(f"[init] model_buy.lgb not found at {buy_path}; will use default 0.5")
        if sell_path.exists():
            _model_sell = lgb.Booster(model_file=str(sell_path))
            print(f"[init] loaded model_sell from {sell_path}")
        else:
            print(f"[init] model_sell.lgb not found at {sell_path}; will use default 0.5")
    else:
        print("[init] lightgbm not installed; all predictions will be 0.5")


# ── Impact spread ─────────────────────────────────────────────────────────────
def _impact_spread_bps(asks: list, bids: list, mid: float) -> Optional[float]:
    """
    Walk top-N levels for a 100,000 JPY notional market order, compute VWAP
    on each side, then return ((vwap_buy - vwap_sell) / mid) * 10000 / 2.
    """
    Q   = 100_000.0
    N   = 5

    def walk(levels):
        remaining = Q
        notional = filled = 0.0
        for price, size in levels[:N]:
            take_jpy = min(remaining, price * size)
            take_sz  = take_jpy / price
            notional += take_jpy
            filled   += take_sz
            remaining -= take_jpy
            if remaining <= 0:
                break
        return notional / filled if filled > 0 else None

    vwap_buy  = walk(asks)
    vwap_sell = walk(bids)
    if vwap_buy is None or vwap_sell is None or mid == 0:
        return None
    return ((vwap_buy - vwap_sell) / mid) * 10_000 / 2


# ── Feature computation (identical to feature_monitor.py) ────────────────────
def _arr(key):
    return np.array([b[key] for b in _bars], dtype=np.float64)

def _fval(arr):
    v = float(arr[-1])
    return v if not math.isnan(v) else float("nan")

def _compute_features() -> dict:
    n = len(_bars)
    if n < 2:
        return {}

    bars = list(_bars)
    closes  = _arr("close");   highs   = _arr("high");    lows    = _arr("low")
    opens   = _arr("open");    volumes = _arr("volume")
    buy_vols= _arr("buy_vol"); sell_vols=_arr("sell_vol")
    ofis    = _arr("ofi");     tcrs    = _arr("trade_cnt_rate")
    mids    = _arr("mid");     vol_imbs= _arr("vol_imb")

    # spreads (NaN where missing)
    spreads = np.array(
        [b["spread"] if b["spread"] is not None else float("nan") for b in bars], dtype=np.float64
    )

    # impact_spread_bps — forward-fill NaN
    imp: list[float] = []
    last_imp = float("nan")
    for b in bars:
        v = b["impact_spread_bps"]
        if v is not None:
            last_imp = v
        imp.append(last_imp)
    impact = np.array(imp, dtype=np.float64)

    f: dict = {}
    nan = float("nan")
    c   = closes[-1]
    hl2 = (highs[-1] + lows[-1]) / 2

    # ── Microstructure (13) ───────────────────────────────────────────────────
    rw = ROLL_N

    # 1. volume_z
    if n >= rw:
        vw = volumes[-rw:]
        std = float(np.std(vw))
        f["volume_z"] = float((volumes[-1] - np.mean(vw)) / std) if std > 0 else 0.0

    # 2. spread_atr_rmean_32
    if n >= max(14, rw) and _TALIB:
        atr = talib.ATR(highs, lows, closes, timeperiod=14)
        with np.errstate(invalid="ignore", divide="ignore"):
            sa = np.where(atr > 0, spreads / atr, np.nan)
        f["spread_atr_rmean_32"] = float(np.nanmean(sa[-rw:]))

    # 3. cum_volume_imb_rmean_32
    if n >= rw:
        f["cum_volume_imb_rmean_32"] = float(np.mean(vol_imbs[-rw:]))

    # 4-5. trade_cnt_rate rmean / rstd
    if n >= rw:
        f["trade_cnt_rate_rmean_32"] = float(np.mean(tcrs[-rw:]))
        f["trade_cnt_rate_rstd_32"]  = float(np.std(tcrs[-rw:]))

    # 6-7. ofi rmean / rstd
    if n >= rw:
        f["ofi_rmean_32"] = float(np.mean(ofis[-rw:]))
        f["ofi_rstd_32"]  = float(np.std(ofis[-rw:]))

    # 8. mid_diff1
    f["mid_diff1"] = float(mids[-1] - mids[-2])

    # 9-10. impact_spread_bps diff1 / pct1
    if not (math.isnan(impact[-1]) or math.isnan(impact[-2])):
        d = float(impact[-1] - impact[-2])
        f["impact_spread_bps_diff1"] = d
        if impact[-2] != 0:
            f["impact_spread_bps_pct1"] = d / abs(float(impact[-2]))

    # 11-12. cum_volume_imb diff1 / pct1
    f["cum_volume_imb_diff1"] = float(vol_imbs[-1] - vol_imbs[-2])
    if vol_imbs[-2] != 0:
        f["cum_volume_imb_pct1"] = float(
            (vol_imbs[-1] - vol_imbs[-2]) / abs(vol_imbs[-2])
        )

    # 13. trade_cnt_rate_diff1
    f["trade_cnt_rate_diff1"] = float(tcrs[-1] - tcrs[-2])

    if not _TALIB:
        return f

    # ── TA-Lib (24) ───────────────────────────────────────────────────────────

    # ADX(14), DX(14)
    if n >= 28:
        f["ADX_14"] = _fval(talib.ADX(highs, lows, closes, timeperiod=14))
        f["DX_14"]  = _fval(talib.DX(highs, lows, closes, timeperiod=14))

    # CCI(14), RSI(14), WILLR(14)
    if n >= 14:
        f["CCI_14"]   = _fval(talib.CCI(highs, lows, closes, timeperiod=14))
        f["RSI_14"]   = _fval(talib.RSI(closes, timeperiod=14))
        f["WILLR_14"] = _fval(talib.WILLR(highs, lows, closes, timeperiod=14))

    # ULTOSC(7, 14, 28)
    if n >= 29:
        f["ULTOSC"] = _fval(
            talib.ULTOSC(highs, lows, closes, timeperiod1=7, timeperiod2=14, timeperiod3=28)
        )

    # MACD(12, 26, 9) — normalized by close
    if n >= 34:
        macd, _, macdhist = talib.MACD(closes, fastperiod=12, slowperiod=26, signalperiod=9)
        f["MACD_macd"]     = float(macd[-1] / c)     if c and not math.isnan(macd[-1])     else nan
        f["MACD_macdhist"] = float(macdhist[-1] / c) if c and not math.isnan(macdhist[-1]) else nan

    # STOCH(5,3,3), STOCHF(5,3)
    if n >= 9:
        sk, sd = talib.STOCH(
            highs, lows, closes,
            fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0,
        )
        fk, _ = talib.STOCHF(highs, lows, closes, fastk_period=5, fastd_period=3, fastd_matype=0)
        f["STOCH_slowk"]  = _fval(sk)
        f["STOCH_slowd"]  = _fval(sd)
        f["STOCHF_fastk"] = _fval(fk)

    # MFI(14)
    if n >= 14:
        f["MFI_14"] = _fval(talib.MFI(highs, lows, closes, volumes, timeperiod=14))

    # Hilbert Transform
    if n >= 32:
        f["HT_DCPERIOD"]          = _fval(talib.HT_DCPERIOD(closes))
        f["HT_DCPHASE"]           = _fval(talib.HT_DCPHASE(closes))
        inph, quad               = talib.HT_PHASOR(closes)
        f["HT_PHASOR_inphase"]    = _fval(inph)
        f["HT_PHASOR_quadrature"] = _fval(quad)

    # LINEARREG (14) — normalized by close
    if n >= 14 and c:
        f["LINEARREG"]           = _fval(talib.LINEARREG(closes, timeperiod=14)) / c
        f["LINEARREG_ANGLE"]     = _fval(talib.LINEARREG_ANGLE(closes, timeperiod=14)) / c
        f["LINEARREG_INTERCEPT"] = _fval(talib.LINEARREG_INTERCEPT(closes, timeperiod=14)) / c
        f["LINEARREG_SLOPE"]     = _fval(talib.LINEARREG_SLOPE(closes, timeperiod=14)) / c

    # KAMA(30), MIDPOINT(14), TEMA(30) — normalize: (val - (hi+lo)/2) / close
    def _norm(val):
        return float((val - hl2) / c) if c else nan

    if n >= 14:
        f["MIDPOINT_14"] = _norm(_fval(talib.MIDPOINT(closes, timeperiod=14)))
    if n >= 30:
        f["KAMA_30"] = _norm(_fval(talib.KAMA(closes, timeperiod=30)))
    if n >= 88:   # TEMA(30) needs 3*(30-1)+1 bars for first non-NaN
        f["TEMA_30"] = _norm(_fval(talib.TEMA(closes, timeperiod=30)))

    # BETA(5)
    if n >= 5:
        f["BETA_5"] = _fval(talib.BETA(highs, lows, timeperiod=5))

    return f


# ── Inference ─────────────────────────────────────────────────────────────────
def _predict(features: dict) -> tuple[float, float]:
    """Return (y_pred_buy, y_pred_sell). Falls back to 0.5 on any error."""
    if not features:
        return 0.5, 0.5

    # Build a single-row feature vector in FEATURE_ORDER
    row = [features.get(name, float("nan")) for name, _ in FEATURE_ORDER]

    y_pred_buy  = 0.5
    y_pred_sell = 0.5

    try:
        if _model_buy is not None:
            X = np.array([row], dtype=np.float64)
            raw = _model_buy.predict(X)
            # predict() returns probabilities directly for binary classifiers
            y_pred_buy = float(raw[0]) if np.ndim(raw) == 1 else float(raw[0][1])
    except Exception as e:
        print(f"[warn] model_buy predict failed: {e}", file=sys.stderr)

    try:
        if _model_sell is not None:
            X = np.array([row], dtype=np.float64)
            raw = _model_sell.predict(X)
            y_pred_sell = float(raw[0]) if np.ndim(raw) == 1 else float(raw[0][1])
    except Exception as e:
        print(f"[warn] model_sell predict failed: {e}", file=sys.stderr)

    return y_pred_buy, y_pred_sell


# ── Simulated fill tracking ───────────────────────────────────────────────────
def _next_order_id() -> int:
    global _local_order_id
    _local_order_id += 1
    return _local_order_id


def _check_fills(bar: dict):
    """
    Check resting orders against the newly closed bar's OHLC.
    BUY LIMIT at p: if bar.low < p → fill at min(p, bar.open)
    SELL LIMIT at p: if bar.high > p → fill at max(p, bar.open)
    """
    global _active_buy_order, _active_sell_order, _position, _realized_pnl

    bar_ts_ns = bar["ts_ns"]

    if _active_buy_order is not None:
        o = _active_buy_order
        p = o["price"]
        if bar["low"] < p:
            fill_price = min(p, bar["open"])
            triggered  = "bar_low" if bar["open"] >= p else "bar_open"
            fill_size  = o["size"]

            _position     += fill_size
            # Entry BUY — no realized PnL yet (realized on exit)
            _fills.append(dict(
                ts_ns       = bar_ts_ns,
                local_order_id = o["order_id"],
                tag         = "ENT_BUY",
                side        = "BUY",
                price       = fill_price,
                size        = fill_size,
                triggered_by= triggered,
                bar_ts_ns   = bar_ts_ns,
            ))
            _fill_rows.append(_fills[-1].copy())
            _active_buy_order = None

    if _active_sell_order is not None:
        o = _active_sell_order
        p = o["price"]
        if bar["high"] > p:
            fill_price = max(p, bar["open"])
            triggered  = "bar_high" if bar["open"] <= p else "bar_open"
            fill_size  = o["size"]

            _position     -= fill_size
            _fills.append(dict(
                ts_ns       = bar_ts_ns,
                local_order_id = o["order_id"],
                tag         = "ENT_SELL",
                side        = "SELL",
                price       = fill_price,
                size        = fill_size,
                triggered_by= triggered,
                bar_ts_ns   = bar_ts_ns,
            ))
            _fill_rows.append(_fills[-1].copy())
            _active_sell_order = None


def _place_orders(bar: dict, buy_limit_price: Optional[float], sell_limit_price: Optional[float]):
    """Place new simulated limit orders (cancel existing before placing)."""
    global _active_buy_order, _active_sell_order

    LOT = 0.01  # minimum GMO lot size in BTC

    if buy_limit_price is not None and not math.isnan(buy_limit_price):
        _active_buy_order = dict(
            order_id = _next_order_id(),
            price    = buy_limit_price,
            size     = LOT,
        )
    else:
        _active_buy_order = None

    if sell_limit_price is not None and not math.isnan(sell_limit_price):
        _active_sell_order = dict(
            order_id = _next_order_id(),
            price    = sell_limit_price,
            size     = LOT,
        )
    else:
        _active_sell_order = None


# ── Bar close ─────────────────────────────────────────────────────────────────
def _close_bar(ts_ns: int):
    global _last_bar, _bar_count

    prices, sizes, sides = _acc_prices[:], _acc_sizes[:], _acc_sides[:]
    _acc_prices.clear(); _acc_sizes.clear(); _acc_sides.clear()

    best_bid  = _gmo_book["best_bid"]
    best_ask  = _gmo_book["best_ask"]
    book_asks = _gmo_book["asks"]
    book_bids = _gmo_book["bids"]
    mid       = (best_bid + best_ask) / 2 if best_bid and best_ask else None

    if not prices:
        # Empty bar: carry forward previous close
        prev = _bars[-1] if _bars else None
        if prev is None:
            return
        c = prev["close"]
        bar = dict(
            ts_ns=ts_ns, open=c, high=c, low=c, close=c,
            volume=0.0, buy_vol=0.0, sell_vol=0.0,
            buy_cnt=0, total_cnt=0, ofi=0.0, trade_cnt_rate=0.5,
            spread=(best_ask - best_bid) if best_bid and best_ask else None,
            mid=mid or c, vol_imb=0.0, impact_spread_bps=None,
        )
    else:
        buy_v  = sum(s for s, sd in zip(sizes, sides) if sd == "BUY")
        sell_v = sum(s for s, sd in zip(sizes, sides) if sd == "SELL")
        tot_v  = buy_v + sell_v
        buy_c  = sum(1 for sd in sides if sd == "BUY")

        spread  = (best_ask - best_bid) if best_bid and best_ask else None
        eff_mid = mid or prices[-1]
        impact  = _impact_spread_bps(book_asks, book_bids, eff_mid) if mid else None

        bar = dict(
            ts_ns=ts_ns,
            open=prices[0], high=max(prices), low=min(prices), close=prices[-1],
            volume=sum(sizes), buy_vol=buy_v, sell_vol=sell_v,
            buy_cnt=buy_c, total_cnt=len(sides),
            ofi=(buy_v - sell_v) / tot_v if tot_v > 0 else 0.0,
            trade_cnt_rate=buy_c / len(sides) if sides else 0.5,
            spread=spread, mid=eff_mid, vol_imb=buy_v - sell_v,
            impact_spread_bps=impact,
        )

    _bars.append(bar)
    _last_bar = bar
    _bar_count += 1

    # Check fills from previous bar's orders against the new bar's OHLC
    _check_fills(bar)

    # Compute features
    _last_features.clear()
    _last_features.update(_compute_features())

    # Run inference
    y_pred_buy, y_pred_sell = _predict(_last_features)

    # Signal generation
    close = bar["close"]
    buy_limit_price  = close - LIMIT_DIST if y_pred_buy  > 0.5 else float("nan")
    sell_limit_price = close + LIMIT_DIST if y_pred_sell > 0.5 else float("nan")

    # Place new simulated orders
    _place_orders(bar, buy_limit_price, sell_limit_price)

    # Theoretical PnL at 0.5bps/side
    theoretical_pnl_0p5bps = 0.0  # populated at fill time; placeholder per-bar

    # Append bar row
    row: dict = {
        "ts_ns":        ts_ns,
        "open":         bar["open"],
        "high":         bar["high"],
        "low":          bar["low"],
        "close":        bar["close"],
        "volume":       bar["volume"],
        "trade_count":  bar["total_cnt"],
    }
    for name, _ in FEATURE_ORDER:
        row[name] = _last_features.get(name, float("nan"))
    row["y_pred_buy"]        = y_pred_buy
    row["y_pred_sell"]       = y_pred_sell
    row["buy_limit_price"]   = buy_limit_price
    row["sell_limit_price"]  = sell_limit_price
    row["position"]          = _position
    row["realized_pnl"]      = _realized_pnl
    row["theoretical_pnl_0p5bps"] = theoretical_pnl_0p5bps

    _bar_rows.append(row)

    # Hourly rotation
    _maybe_rotate_bar_file(ts_ns)


# ── Parquet output ─────────────────────────────────────────────────────────────
def _bar_parquet_schema():
    fields = [
        pa.field("ts_ns",        pa.int64()),
        pa.field("open",         pa.float64()),
        pa.field("high",         pa.float64()),
        pa.field("low",          pa.float64()),
        pa.field("close",        pa.float64()),
        pa.field("volume",       pa.float64()),
        pa.field("trade_count",  pa.int64()),
    ]
    for name, _ in FEATURE_ORDER:
        fields.append(pa.field(name, pa.float64()))
    fields += [
        pa.field("y_pred_buy",        pa.float64()),
        pa.field("y_pred_sell",       pa.float64()),
        pa.field("buy_limit_price",   pa.float64()),
        pa.field("sell_limit_price",  pa.float64()),
        pa.field("position",          pa.float64()),
        pa.field("realized_pnl",      pa.float64()),
        pa.field("theoretical_pnl_0p5bps", pa.float64()),
    ]
    return pa.schema(fields)


def _fill_parquet_schema():
    return pa.schema([
        pa.field("ts_ns",          pa.int64()),
        pa.field("local_order_id", pa.int64()),
        pa.field("tag",            pa.string()),
        pa.field("side",           pa.string()),
        pa.field("price",          pa.float64()),
        pa.field("size",           pa.float64()),
        pa.field("triggered_by",   pa.string()),
        pa.field("bar_ts_ns",      pa.int64()),
    ])


def _flush_bar_rows(date_str: str, hour: int):
    """Write accumulated bar rows to parquet."""
    global _bar_rows
    if not _ARROW or not _bar_rows:
        _bar_rows = []
        return

    out_dir = DATA_DIR / "bars" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"bars_{hour:02d}0000.parquet"

    schema = _bar_parquet_schema()
    cols: dict = {f.name: [] for f in schema}
    for r in _bar_rows:
        for f in schema:
            cols[f.name].append(r.get(f.name, None))

    arrays = []
    for f in schema:
        if pa.types.is_int64(f.type):
            arrays.append(pa.array(cols[f.name], type=pa.int64()))
        else:
            arrays.append(pa.array(cols[f.name], type=pa.float64()))

    table = pa.table({f.name: arr for f, arr in zip(schema, arrays)}, schema=schema)

    if path.exists():
        existing = pq.read_table(path)
        table = pa.concat_tables([existing, table])
    pq.write_table(table, path, compression="snappy")
    _bar_rows = []


def _flush_fill_rows(date_str: str):
    """Write accumulated fill rows to parquet."""
    global _fill_rows
    if not _ARROW or not _fill_rows:
        _fill_rows = []
        return

    out_dir = DATA_DIR / "trades_sim" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "fills.parquet"

    schema = _fill_parquet_schema()
    cols: dict = {f.name: [] for f in schema}
    for r in _fill_rows:
        for f in schema:
            cols[f.name].append(r.get(f.name, None))

    arrays = []
    for f in schema:
        if pa.types.is_int64(f.type):
            arrays.append(pa.array(cols[f.name], type=pa.int64()))
        elif pa.types.is_string(f.type):
            arrays.append(pa.array(cols[f.name], type=pa.string()))
        else:
            arrays.append(pa.array(cols[f.name], type=pa.float64()))

    table = pa.table({f.name: arr for f, arr in zip(schema, arrays)}, schema=schema)

    if path.exists():
        existing = pq.read_table(path)
        table = pa.concat_tables([existing, table])
    pq.write_table(table, path, compression="snappy")
    _fill_rows = []


def _maybe_rotate_bar_file(ts_ns: int):
    global _current_bar_hour, _current_bar_date

    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    hour     = dt.hour

    if _current_bar_hour is None:
        _current_bar_hour = hour
        _current_bar_date = date_str
        return

    if hour != _current_bar_hour or date_str != _current_bar_date:
        # Flush the completed hour
        _flush_bar_rows(_current_bar_date, _current_bar_hour)
        _flush_fill_rows(_current_bar_date)
        _current_bar_hour = hour
        _current_bar_date = date_str


def _shutdown_flush():
    """Flush remaining rows on shutdown."""
    if _current_bar_date and _current_bar_hour is not None:
        _flush_bar_rows(_current_bar_date, _current_bar_hour)
        _flush_fill_rows(_current_bar_date)


# ── Display ───────────────────────────────────────────────────────────────────
_W = 60   # box inner width

def _box(text, width=_W):
    return f"║  {text:<{width}}║"

def _sep(width=_W):
    return f"╠{'═'*(width+3)}╣"

def _top(title, width=_W):
    return f"╔{'═'*(width+3)}╗\n║  {title:<{width}}║"

def _bot(width=_W):
    return f"╚{'═'*(width+3)}╝"

def _fp(v, dec=2):
    """Format a float value; show N/A for nan/None."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "N/A"
    if dec == 0:
        return f"{v:>14,.0f}"
    return f"{v:>10,.{dec}f}"

def _frow(name, v, dec=4):
    val = "N/A" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:+.{dec}f}"
    return _box(f"{name:<30} {val:>25}")


def _render() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [_top(f"GMO BTC/JPY ML Pipeline  {now}")]

    # ── Prices ───────────────────────────────────────────────────────────────
    b = _gmo_book; u = _usdjpy; c = _btcusd
    bid_g = b["best_bid"]; ask_g = b["best_ask"]
    bid_u = u["bid"];      ask_u = u["ask"]
    bid_c = c["bid"];      ask_c = c["ask"]

    lines += [
        _sep(),
        _box("PRICES"),
        _box(f"  GMO  BTC/JPY  bid {_fp(bid_g,0):>14}  ask {_fp(ask_g,0):>14}"),
        _box(f"  GMO  USD/JPY  bid {_fp(bid_u,3):>14}  ask {_fp(ask_u,3):>14}"),
        _box(f"  CB   BTC/USD  bid {_fp(bid_c,2):>14}  ask {_fp(ask_c,2):>14}"),
    ]

    # ── Current bar ───────────────────────────────────────────────────────────
    lines.append(_sep())
    b2 = _last_bar
    if b2:
        ts = datetime.fromtimestamp(b2["ts_ns"] / 1e9, tz=timezone.utc).strftime("%H:%M:%S")
        lines += [
            _box(f"BAR #{_bar_count}  @{ts}  trades={b2['total_cnt']}"),
            _box(f"  O {b2['open']:>12,.0f}  H {b2['high']:>12,.0f}"),
            _box(f"  L {b2['low']:>12,.0f}  C {b2['close']:>12,.0f}"),
            _box(f"  vol {b2['volume']:>8.4f}  OFI {b2['ofi']:>+7.3f}  TCR {b2['trade_cnt_rate']:>6.3f}"),
        ]
    else:
        lines.append(_box("BAR  (waiting for first bar...)"))

    # ── Predictions & signals ─────────────────────────────────────────────────
    lines.append(_sep())
    # Retrieve last predictions from latest row
    y_pred_buy  = float("nan")
    y_pred_sell = float("nan")
    buy_lp      = float("nan")
    sell_lp     = float("nan")
    if _bar_rows:
        last_row = _bar_rows[-1]
        y_pred_buy  = last_row.get("y_pred_buy",       float("nan"))
        y_pred_sell = last_row.get("y_pred_sell",      float("nan"))
        buy_lp      = last_row.get("buy_limit_price",  float("nan"))
        sell_lp     = last_row.get("sell_limit_price", float("nan"))

    lines += [
        _box("PREDICTIONS & SIGNALS"),
        _box(f"  y_pred_buy  {_fp(y_pred_buy,  4):>20}   y_pred_sell {_fp(y_pred_sell, 4):>20}"),
        _box(f"  buy_limit   {_fp(buy_lp,  0):>20}   sell_limit  {_fp(sell_lp, 0):>20}"),
    ]

    # ── Position & PnL ───────────────────────────────────────────────────────
    lines.append(_sep())
    lines += [
        _box("POSITION & PnL"),
        _box(f"  position     {_position:>+.4f} BTC"),
        _box(f"  realized_pnl {_realized_pnl:>+,.0f} JPY"),
    ]

    # ── Active orders ─────────────────────────────────────────────────────────
    lines.append(_sep())
    lines.append(_box("ACTIVE ORDERS"))
    if _active_buy_order:
        o = _active_buy_order
        lines.append(_box(f"  BUY  LIMIT #{o['order_id']}  price={o['price']:,.0f}  size={o['size']:.4f}"))
    else:
        lines.append(_box("  BUY  LIMIT: none"))
    if _active_sell_order:
        o = _active_sell_order
        lines.append(_box(f"  SELL LIMIT #{o['order_id']}  price={o['price']:,.0f}  size={o['size']:.4f}"))
    else:
        lines.append(_box("  SELL LIMIT: none"))

    # ── Recent fills ─────────────────────────────────────────────────────────
    lines.append(_sep())
    lines.append(_box("RECENT FILLS (last 5)"))
    recent = _fills[-5:] if _fills else []
    if recent:
        for fl in reversed(recent):
            ts_f = datetime.fromtimestamp(fl["ts_ns"] / 1e9, tz=timezone.utc).strftime("%H:%M:%S")
            lines.append(_box(
                f"  {ts_f}  {fl['tag']:<10} {fl['side']:<4} "
                f"px={fl['price']:,.0f}  sz={fl['size']:.4f}  by={fl['triggered_by']}"
            ))
    else:
        lines.append(_box("  (none yet)"))

    lines.append(_bot())
    return "\n".join(lines)


def _display():
    print("\033[H\033[J" + _render(), end="", flush=True)


# ── Bar timer ─────────────────────────────────────────────────────────────────
async def _bar_timer():
    """Close a bar every BAR_SEC seconds, aligned to wall-clock boundaries."""
    while True:
        now  = time.time()
        wait = math.ceil(now / BAR_SEC) * BAR_SEC - now
        await asyncio.sleep(max(wait, 0.001))

        ts_ns = int(time.time_ns())
        _close_bar(ts_ns)
        _display()


# ── GMO Coin — orderbooks + trades ───────────────────────────────────────────
async def _gmo_coin():
    uri = "wss://api.coin.z.com/ws/public/v1"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps(
                    {"command": "subscribe", "channel": "orderbooks", "symbol": "BTC_JPY"}
                ))
                await asyncio.sleep(1)   # GMO rate limit: 1 s between subscribes
                await ws.send(json.dumps(
                    {"command": "subscribe", "channel": "trades", "symbol": "BTC_JPY"}
                ))
                async for raw in ws:
                    d = json.loads(raw)
                    ch = d.get("channel")
                    if ch == "orderbooks":
                        bids = [(float(x["price"]), float(x["size"])) for x in d.get("bids", [])]
                        asks = [(float(x["price"]), float(x["size"])) for x in d.get("asks", [])]
                        _gmo_book["bids"]     = bids[:BOOK_KEEP]
                        _gmo_book["asks"]     = asks[:BOOK_KEEP]
                        _gmo_book["best_bid"] = bids[0][0] if bids else None
                        _gmo_book["best_ask"] = asks[0][0] if asks else None
                    elif ch == "trades":
                        price = float(d["price"])
                        size  = float(d["size"])
                        side  = d.get("side", "BUY")
                        _acc_prices.append(price)
                        _acc_sizes.append(size)
                        _acc_sides.append(side)
        except Exception as e:
            print(f"\n[gmocoin] {e}", file=sys.stderr)
            await asyncio.sleep(3)


# ── GMO FX — USD_JPY ticker ──────────────────────────────────────────────────
async def _gmo_fx():
    uri = "wss://forex-api.coin.z.com/ws/public/v1"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps(
                    {"command": "subscribe", "channel": "ticker", "symbol": "USD_JPY"}
                ))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("symbol") == "USD_JPY" and "bid" in d:
                        _usdjpy["bid"] = float(d["bid"])
                        _usdjpy["ask"] = float(d["ask"])
        except Exception as e:
            print(f"\n[gmofx] {e}", file=sys.stderr)
            await asyncio.sleep(3)


# ── Coinbase — BTC-USD level2 ────────────────────────────────────────────────
async def _coinbase():
    uri = "wss://advanced-trade-ws.coinbase.com"
    while True:
        try:
            async with websockets.connect(uri, max_size=10 * 1024 * 1024) as ws:
                await ws.send(json.dumps(
                    {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"}
                ))
                _cb_bids.clear(); _cb_asks.clear()
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") != "l2_data":
                        continue
                    for event in d.get("events", []):
                        if event.get("type") == "snapshot":
                            _cb_bids.clear(); _cb_asks.clear()
                        for upd in event.get("updates", []):
                            side  = upd.get("side")
                            price = upd.get("price_level")
                            size  = float(upd.get("new_quantity", 0))
                            if side == "bid":
                                if size == 0: _cb_bids.pop(price, None)
                                else:         _cb_bids[price] = size
                            elif side == "offer":
                                if size == 0: _cb_asks.pop(price, None)
                                else:         _cb_asks[price] = size
                    if _cb_bids and _cb_asks:
                        _btcusd["bid"] = float(max(_cb_bids, key=float))
                        _btcusd["ask"] = float(min(_cb_asks, key=float))
        except Exception as e:
            print(f"\n[coinbase] {e}", file=sys.stderr)
            await asyncio.sleep(3)


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    _init()
    print("\033[2J\033[H", end="", flush=True)
    try:
        await asyncio.gather(_gmo_coin(), _gmo_fx(), _coinbase(), _bar_timer())
    finally:
        _shutdown_flush()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
