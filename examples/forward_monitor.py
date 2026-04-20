#!/usr/bin/env python3
"""
Forward-test price monitor.

Subscribes to:
  - GMO Coin  BTC_JPY  (orderbooks)
  - GMO FX    USD_JPY  (ticker)
  - Coinbase  BTC-USD  (l2_data)

Displays:
  - GMO BTC/JPY bid/ask
  - GMO USD/JPY bid/ask
  - Coinbase BTC/USD bid/ask
  - Coinbase BTC/USD * USD/JPY (derived BTC/JPY)
  - Basis: GMO vs derived (bps)
"""

import asyncio
import json
import sys
from datetime import datetime, timezone

import websockets

# ── shared state ──────────────────────────────────────────────────────────────
state = {
    "btcjpy": {"bid": None, "ask": None},
    "usdjpy": {"bid": None, "ask": None},
    "btcusd": {"bid": None, "ask": None},
}

# Coinbase level-2 book: price_str -> size_float
_cb_bids: dict[str, float] = {}
_cb_asks: dict[str, float] = {}

_lock = asyncio.Lock()


# ── display ───────────────────────────────────────────────────────────────────
def _fmt(v, dec: int) -> str:
    if v is None:
        return "     N/A"
    if dec == 0:
        return f"{v:>14,.0f}"
    return f"{v:>14,.{dec}f}"


def _render() -> str:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    b = state["btcjpy"]
    u = state["usdjpy"]
    c = state["btcusd"]

    lines = [
        f"\033[H",  # cursor home (no erase — overwrites in place)
        f"╔═══════════════════════════════════════╗",
        f"║  BTC/JPY Forward Monitor  {now} UTC  ║",
        f"╠═══════════════════════════════════════╣",
        f"║  GMO  BTC/JPY  bid {_fmt(b['bid'], 0)}  ║",
        f"║                ask {_fmt(b['ask'], 0)}  ║",
        f"║  GMO  USD/JPY  bid {_fmt(u['bid'], 3)}  ║",
        f"║                ask {_fmt(u['ask'], 3)}  ║",
        f"║  CB   BTC/USD  bid {_fmt(c['bid'], 2)}  ║",
        f"║                ask {_fmt(c['ask'], 2)}  ║",
        f"╠═══════════════════════════════════════╣",
    ]

    if all(v is not None for v in [u["bid"], u["ask"], c["bid"], c["ask"]]):
        der_bid = c["bid"] * u["bid"]
        der_ask = c["ask"] * u["ask"]
        lines += [
            f"║  CB×FX BTC/JPY bid {_fmt(der_bid, 0)}  ║",
            f"║                ask {_fmt(der_ask, 0)}  ║",
        ]
        if b["bid"] and b["ask"]:
            gmo_mid = (b["bid"] + b["ask"]) / 2
            der_mid = (der_bid + der_ask) / 2
            basis = (gmo_mid / der_mid - 1) * 10_000
            lines.append(
                f"║  Basis (GMO/CB×FX) {basis:>+13.2f} bps  ║"
            )
        else:
            lines.append(f"║  Basis             {'N/A':>13}      ║")
    else:
        lines += [
            f"║  CB×FX BTC/JPY     {'N/A':>13}      ║",
            f"║  Basis             {'N/A':>13}      ║",
        ]

    lines.append(f"╚═══════════════════════════════════════╝")
    return "\n".join(lines)


async def display():
    async with _lock:
        print(_render(), end="", flush=True)


# ── GMO Coin — BTC_JPY orderbooks ─────────────────────────────────────────────
async def gmo_coin():
    uri = "wss://api.coin.z.com/ws/public/v1"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps({
                    "command": "subscribe",
                    "channel": "orderbooks",
                    "symbol": "BTC_JPY",
                }))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") != "orderbooks":
                        continue
                    bids = d.get("bids", [])
                    asks = d.get("asks", [])
                    if bids:
                        state["btcjpy"]["bid"] = float(bids[0]["price"])
                    if asks:
                        state["btcjpy"]["ask"] = float(asks[0]["price"])
                    await display()
        except Exception as e:
            print(f"\n[gmocoin] {e}", file=sys.stderr)
            await asyncio.sleep(3)


# ── GMO FX — USD_JPY ticker ───────────────────────────────────────────────────
async def gmo_fx():
    uri = "wss://forex-api.coin.z.com/ws/public/v1"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps({
                    "command": "subscribe",
                    "channel": "ticker",
                    "symbol": "USD_JPY",
                }))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("symbol") != "USD_JPY" or "bid" not in d:
                        continue
                    state["usdjpy"]["bid"] = float(d["bid"])
                    state["usdjpy"]["ask"] = float(d["ask"])
                    await display()
        except Exception as e:
            print(f"\n[gmofx] {e}", file=sys.stderr)
            await asyncio.sleep(3)


# ── Coinbase — BTC-USD l2_data ────────────────────────────────────────────────
def _apply_cb_updates(events: list):
    for event in events:
        if event.get("type") == "snapshot":
            _cb_bids.clear()
            _cb_asks.clear()
        for upd in event.get("updates", []):
            side = upd.get("side")
            price = upd.get("price_level")
            size = float(upd.get("new_quantity", 0))
            if side == "bid":
                if size == 0.0:
                    _cb_bids.pop(price, None)
                else:
                    _cb_bids[price] = size
            elif side == "offer":
                if size == 0.0:
                    _cb_asks.pop(price, None)
                else:
                    _cb_asks[price] = size

    if _cb_bids and _cb_asks:
        best_bid = max(_cb_bids, key=float)
        best_ask = min(_cb_asks, key=float)
        state["btcusd"]["bid"] = float(best_bid)
        state["btcusd"]["ask"] = float(best_ask)
        return True
    return False


async def coinbase():
    uri = "wss://advanced-trade-ws.coinbase.com"
    while True:
        try:
            async with websockets.connect(uri, max_size=10 * 1024 * 1024) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": ["BTC-USD"],
                    "channel": "level2",
                }))
                _cb_bids.clear()
                _cb_asks.clear()
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") != "l2_data":
                        continue
                    updated = _apply_cb_updates(d.get("events", []))
                    if updated:
                        await display()
        except Exception as e:
            print(f"\n[coinbase] {e}", file=sys.stderr)
            await asyncio.sleep(3)


# ── main ──────────────────────────────────────────────────────────────────────
async def main():
    print("\033[2J\033[H", end="", flush=True)  # clear screen once
    await asyncio.gather(gmo_coin(), gmo_fx(), coinbase())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
