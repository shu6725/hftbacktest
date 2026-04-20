# GMO Coin BTC_JPY Leverage — Forward-Test Simulation Spec (Rust / hftbacktest)

Phase-1 spec: **no real orders**. Live WS market data only; fills are simulated from bar OHLC via the same rule as the Python backtest. When we later flip to real execution, we replace the stub inside `connector-gmocoin/src/rest_exec.rs` with the real REST client — no code changes in `bot-forward-test`.

---

## 1. Objectives

- Run 24/7 against live GMO Coin WS feed for days without intervention.
- Produce parquet files with bars, features, predictions, and simulated trades that are directly reconcilable against the Python backtest.
- Exercise the full hftbacktest stack (connector ↔ bot ↔ shared memory ↔ `LiveBot` API) so that the Phase-2 switch to real execution is mechanical.
- Capture raw WS messages to disk for offline replay / debugging.

---

## 2. What changes vs. the full live spec

| Component | Full live (Phase-2) | Simulation (Phase-1, this spec) |
|---|---|---|
| Public WS (orderbooks/trades) | Real | **Real (identical)** |
| Private WS (executions) | Real | Not used (no real orders) |
| REST order submit/cancel | Real | **Stub — no HTTP call, return synthetic ACK** |
| Fill detection | Private WS events | **Simulated: next-bar `hi > sell_price` / `lo < buy_price`** |
| Position, PnL | From exchange | Tracked locally from simulated fills |
| Fees | 0 (GMO leverage) | 0 realized; 0.5 bps/side theoretical column |
| `--dry-run` flag | Available | Is the only mode |

Everything else (bar builder, feature engine, models, signal logic, persistence) is identical to Phase-2.

---

## 3. Process Topology (unchanged from full spec, §2)

Two binaries, Iceoryx2 shm between them. Only the connector internals differ.

```
┌──────────────────────────────────┐        Iceoryx2 shm         ┌──────────────────────────────┐
│   connector-gmocoin              │ ──── LiveEvent ───────────> │   bot-forward-test           │
│   (SIM mode)                     │                             │                              │
│   - WS public (real)             │                             │   (unchanged from live spec) │
│   - WS private: NOT subscribed   │                             │                              │
│   - REST exec: STUB              │ <──── OrderRequest ──────── │                              │
│   - Raw WS recorder → .jsonl.gz  │ ──── Synthetic Ack ───────> │                              │
└──────────────────────────────────┘                             └──────────────────────────────┘
```

---

## 4. Connector in SIM mode (`connector-gmocoin`)

### 4.1 Public WS (real, identical to live)
- Connect `wss://api.coin.z.com/ws/public/v1`
- Subscribe `orderbooks` + `trades` for `BTC_JPY`
- Parse messages → publish hftbacktest `LiveEvent::Depth` and `LiveEvent::Trade` via Iceoryx2
- Reconnect with exponential backoff on drop; emit `ConnectionInterrupted` if down >30s

### 4.2 Raw recorder (new, SIM-only but keep in Phase-2 too)
- Every received WS frame is appended to `data/raw/{date}/{channel}.jsonl.gz` with local recv timestamp prefix (nanoseconds)
- Format: `{recv_ts_ns}\t{raw_json}\n` — identical to how hftbacktest's Python replay expects it, so these files can be converted and replayed with the backtest engine later
- Rotate daily at 00:00 UTC

### 4.3 Private WS
- **Not subscribed.** No `ws-auth` call, no API key needed for Phase-1.
- Config: `enable_private = false`.

### 4.4 REST exec stub (`rest_exec.rs`)
The trait seen by the rest of the connector:
```rust
pub trait RestExec: Send + Sync {
    async fn submit(&self, req: OrderSubmitRequest) -> Result<OrderAck>;
    async fn cancel(&self, gmo_order_id: u64) -> Result<()>;
    async fn modify(&self, gmo_order_id: u64, new_price: f64) -> Result<()>;
}
```
Two implementations:
- `RealRestExec` — actual HTTP calls (Phase-2).
- `StubRestExec` — Phase-1. Each call returns immediately with a synthetic `OrderAck { gmo_order_id: self.next_id() }`. No HTTP traffic.

Selected at connector startup via config:
```toml
exec_mode = "stub"   # or "real" in Phase-2
```

### 4.5 Simulated fills (inside the connector)
This is the core SIM feature: fills are driven by **bar OHLC derived from the same trade stream the bot uses**, not by exchange executions.

- The connector keeps a running 5-second bar builder **in parallel with** the bot's. (Yes, duplicated — keeps the connector the sole authority on fill events, mirroring the real architecture.)
- On each bar close, for every resting stub order:
  - BUY LIMIT at `p`: if `bar.low < p` → fill at `p` (or `bar.open` if `bar.open < p`, matching the backtest's gap convention)
  - SELL LIMIT at `p`: if `bar.high > p` → fill at `p` (or `bar.open` if `bar.open > p`)
  - MARKET: fill at `bar.close` of the bar in which the order arrived
- On fill → send `LiveEvent::Order(status=Filled, ...)` to the bot via Iceoryx2, just like the real private WS would.
- Partial fills: not simulated in Phase-1 (always all-or-nothing). Flag as a known simplification.
- Slippage: zero in Phase-1. Document explicitly so we know what to add later.

### 4.6 Why simulate fills inside the connector (not the bot)
Two reasons:
1. **Phase-2 diff is zero in the bot.** In real execution the bot just consumes `LiveEvent::Order` from Iceoryx2 — it doesn't care whether the producer is the exchange's private WS or our simulator.
2. **One source of truth for bars.** Both connector and bot build bars from the same trade stream; fill simulation uses the connector's bars, avoiding a race where the bot decides "filled" before the connector has seen the trade.

---

## 5. Bot (`bot-forward-test`) — identical to full spec

No conditional code. The bot does not know it's in simulation. It calls `hbt.submit_order(...)` and receives `Order` events exactly as it would in live. All Phase-1 logic reads exactly the same as Phase-2 §5 of the prior spec.

Relevant detail: **LightGBM inference latency is a non-issue at 5s cadence.** Benchmark reference: 128-feature / 1000-tree LightGBM on CPU shows median latency ~45μs and 99th percentile ~51μs with oneDAL. Our model is ~37 features, ~100 trees — expect single-digit μs per prediction. Two models per bar ≈ 30μs worst case, i.e. 6e-6 of the 5s budget. The real budget on each bar close is dominated by TA-Lib rolling recomputation and parquet appends; target <100ms total on bar close. No async offload of inference is needed.

---

## 6. Data outputs (runs for days, so size matters)

```
data/
├── raw/
│   └── 2026-04-20/
│       ├── orderbooks.jsonl.gz        # raw WS, rotated daily
│       └── trades.jsonl.gz
├── bars/
│   └── 2026-04-20/bars.parquet        # one row per 5s bar
├── trades_sim/
│   └── 2026-04-20/fills.parquet       # simulated fills
└── logs/
    └── 2026-04-20/bot.log
```

### 6.1 `bars.parquet` schema
```
ts_ns (i64, UTC ns), open, high, low, close, volume, trade_count,
<37 feature columns in feature_columns.json order>,
y_pred_buy, y_pred_sell,
buy_limit_price, sell_limit_price,
position, realized_pnl, theoretical_pnl_0p5bps
```

### 6.2 `fills.parquet` schema
```
ts_ns, local_order_id, tag (ENT_BUY/ENT_SELL/EXT_BUY/EXT_SELL),
side, price, size, triggered_by (bar_low|bar_high|bar_open), bar_ts_ns
```

### 6.3 Rotation & retention
- Parquet files roll hourly inside each day directory to keep write amplification sane.
- No auto-deletion; manual cleanup.

---

## 7. Operational

### 7.1 CLI
```
# terminal 1
cargo run --release -p connector-gmocoin -- --config config/gmocoin.sim.toml

# terminal 2
cargo run --release -p bot-forward-test -- --data-dir ./data
```

### 7.2 Config (`gmocoin.sim.toml`)
```toml
exec_mode          = "stub"
enable_private     = false
public_stream_url  = "wss://api.coin.z.com/ws/public/v1"
raw_recorder       = true
raw_dir            = "./data/raw"

[[instruments]]
symbol    = "BTC_JPY"
tick_size = 1.0
lot_size  = 0.01
```

### 7.3 Running for days
- Wrap both binaries in `systemd` user units or `tmux` panes with `restart=on-failure`.
- Daily log rotation via `logrotate` or `tracing-appender`'s daily roller.
- Disk budget estimate: raw orderbooks ≈ 1–2 GB/day gzipped; bars ≈ tiny. Plan for ~50 GB/month.
- Memory: rolling buffers + depth hashmap → expect <500 MB steady-state.

### 7.4 Health checks
- Bot emits a heartbeat line to stdout every closed bar.
- Connector emits WS connection status to stdout on every reconnect.
- Discord webhook on: connector disconnect >30s, bot crash, daily PnL summary at 00:00 UTC.

---

## 8. Acceptance Criteria (Phase-1)

1. `cargo build --release` clean on Ubuntu 24.04.
2. Continuous 72-hour run produces 72×3600/5 = 51,840 bar rows with strictly monotonic, gap-free 5s timestamps.
3. Feature columns match `feature_columns.json`.
4. Pinned-vector model predictions match Python to 1e-6.
5. At least one entry-then-exit round-trip observed in `fills.parquet`; manual spot-check confirms PnL matches `close_of_exit_bar - close_of_entry_bar ± limit_distance` as expected.
6. Raw recorder files are replay-compatible: a short offline script can feed one day's `trades.jsonl.gz` into the Python backtest and reproduce the same predictions (modulo tiny float differences) on the same timestamps.
7. Connector can be killed and restarted without the bot crashing (bot sees `ConnectionInterrupted`, cancels all, waits for new Depth events, resumes).
8. Memory usage stable (no leak) over 72h.

---

## 9. Phase-2 switchover checklist

When we're ready to go live:
1. Implement `RealRestExec` in `connector-gmocoin/src/rest_exec.rs` (HMAC-SHA256 auth, REST endpoints).
2. Subscribe to private WS `executionEvents`; route to `LiveEvent::Order`.
3. **Delete the internal bar-based fill simulator**, or keep behind `exec_mode=stub` for offline tests.
4. Flip `exec_mode = "real"` in config.
5. Expected diff in `bot-forward-test`: zero.

---

## 10. Feature definitions (resolved)

Bar interval: **5 seconds**.

### 10.1 OFI (Order Flow Imbalance — trade-side variant used here)

Not the Cont/Kukanov/Stoikov book-update form. **For this project OFI is a per-bar trade-flow imbalance ratio.**

Within each 5s bar, classify every trade as buy or sell (use the exchange-provided `side` field on the `trades` WS channel; `BUY` = aggressor bought = taker buy):

```
buy_volume  = sum(size for t in bar if t.side == "BUY")
sell_volume = sum(size for t in bar if t.side == "SELL")
total_volume = buy_volume + sell_volume

ofi = (buy_volume - sell_volume) / total_volume   if total_volume > 0
ofi = 0.0                                         if total_volume == 0
```

Range: [-1, +1]. Dimensionless.

### 10.2 `impact_spread_bps` — Q=100,000 JPY, N=5

Compute the hypothetical VWAP execution price for a 100,000 JPY notional market order walking up to 5 levels of the book, for each side, then average. All computed against the current orderbook snapshot (latest at bar close).

```
Q_jpy = 100_000
N     = 5
mid   = (best_bid + best_ask) / 2

# BUY side — walk asks
remaining = Q_jpy
notional  = 0.0
filled_sz = 0.0
for (price, size) in asks[:N]:
    take_jpy = min(remaining, price * size)
    take_sz  = take_jpy / price
    notional  += take_jpy
    filled_sz += take_sz
    remaining -= take_jpy
    if remaining <= 0:
        break
vwap_buy = notional / filled_sz            # if filled_sz > 0 else NaN, log and skip bar

# SELL side — walk bids, symmetric
vwap_sell = ...  # same pattern on bids[:N]

impact_spread_bps = ((vwap_buy - vwap_sell) / mid) * 10_000 / 2
                  = ((vwap_buy - mid) + (mid - vwap_sell)) / mid * 10_000 / 2
```

The `/2` reflects the "average of the two sides" in the user's definition ("その平均を取る"). Port this exact formula.

Edge cases:
- If top-N book depth can't absorb 100,000 JPY → use whatever fills are available, VWAP over what was filled. If zero depth on a side, log and emit NaN for this bar; feature pipeline should forward-fill with the last valid value (do not drop the bar — that breaks the gap-free invariant).

### 10.3 `trade_cnt_rate`

Bar interval for Phase-1 is 5s fixed. Denominator is the **total trade count** in the bar, not seconds:

```
buy_cnt   = count of trades in bar with side == "BUY"
total_cnt = count of all trades in bar

trade_cnt_rate = buy_cnt / total_cnt   if total_cnt > 0
trade_cnt_rate = 0.5                   if total_cnt == 0   # neutral
```

Note: this is a redefinition from the original training pipeline. Phase-1 doubles as data-collection for retraining; the current models will be used only for sanity checks until we have enough 5s-bar data (target: 2+ weeks) to retrain `model_buy` / `model_sell` on the new feature definition.

### 10.4 TA-Lib binding

Use the `talib` crate (v0.1.x on crates.io, `talib-sys` underneath for bindgen'd C linkage). Both `talib` (Rust) and Python `ta-lib` wrap the **same TA-Lib C library** (version 0.4.x or 0.6.x), so numerical parity with the Python training pipeline is byte-level on the same C version. Pin both Python and Rust sides to TA-Lib **0.4.0** (stable, widely available) unless training used 0.6.x.

Avoid `kand` and similar Rust-native re-implementations — they risk silent numerical drift.

System dep: `ta-lib` C library installed at `/usr/local/lib` (from source tarball or `apt` on recent Ubuntu). Document this in the README.

### 10.5 hftbacktest commit

Pin an explicit commit hash at implementation start — do not use a branch ref. Record in `Cargo.toml` and in the README.

---

## 11. Phase-1 doubles as data collection

Because `trade_cnt_rate` and the feature pipeline are partially redefined, the Phase-1 run is a **data-collection + sanity-check** phase. Concretely:

- All 37 features are computed and persisted with the new definitions.
- Current `model_buy.lgb` / `model_sell.lgb` (trained on the old pipeline) are loaded and run for pipeline smoke-testing, but their predictions should not be trusted as profitable signals.
- After ~2 weeks of 5s bars are collected, retrain both models on the new feature definitions and swap in. Persist the old models aside for comparison.
- Only after retraining + offline validation should Phase-2 (real execution) be triggered.
