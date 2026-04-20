use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, OrderId, Status};
use tracing::error;

use crate::{
    bitbank::{BitbankError, msg::rest::OrderResponse, msg::stream::SpotOrder},
    connector::GetOrders,
    utils::{RefSymbolOrderId, SymbolOrderId},
};

#[allow(dead_code)]

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    /// Set after the REST response is received; None while the REST call is in-flight.
    bitbank_order_id: Option<i64>,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

#[derive(Default, Debug)]
pub struct OrderManager {
    /// (symbol, hft_order_id) → OrderExt
    orders: HashMap<SymbolOrderId, OrderExt>,
    /// bitbank_order_id → (symbol, hft_order_id)
    bitbank_id_map: HashMap<i64, SymbolOrderId>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Submit flow ──────────────────────────────────────────────────────────

    /// Registers a pending order before the REST call.  Returns false if there
    /// is already an order with the same (symbol, order_id) in-flight.
    pub fn prepare(&mut self, symbol: String, order: Order) -> bool {
        let key = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.orders.contains_key(&key) {
            return false;
        }
        self.orders.insert(
            key,
            OrderExt {
                symbol,
                order,
                bitbank_order_id: None,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        true
    }

    /// Called after a successful submit REST response; wires up the server-side ID.
    pub fn update_from_submit(
        &mut self,
        symbol: &str,
        hft_order_id: OrderId,
        resp: &OrderResponse,
    ) -> Option<Order> {
        let key = RefSymbolOrderId::new(symbol, hft_order_id);
        let order_ext = self.orders.get_mut(&key)?;

        order_ext.bitbank_order_id = Some(resp.order_id);
        self.bitbank_id_map
            .insert(resp.order_id, SymbolOrderId::new(symbol.to_string(), hft_order_id));

        let ts = resp.ordered_at * 1_000_000;
        if ts >= order_ext.order.exch_timestamp {
            order_ext.order.exch_timestamp = ts;
            order_ext.order.status = resp.status;
            order_ext.order.req = Status::None;
            order_ext.order.qty = resp.start_amount;
            order_ext.order.leaves_qty = resp.remaining_amount;
            order_ext.order.exec_qty = resp.executed_amount;
            order_ext.order.side = resp.side;
            order_ext.order.order_type = resp.order_type;
        }

        self.maybe_remove_and_return(symbol, hft_order_id, false)
    }

    pub fn update_submit_fail(&mut self, symbol: &str, hft_order_id: OrderId) -> Option<Order> {
        let key = RefSymbolOrderId::new(symbol, hft_order_id);
        let order_ext = self.orders.get_mut(&key)?;
        order_ext.order.req = Status::None;
        order_ext.order.status = Status::Expired;
        self.maybe_remove_and_return(symbol, hft_order_id, false)
    }

    // ── Cancel flow ──────────────────────────────────────────────────────────

    /// Returns the bitbank order_id needed to send a cancel REST request.
    pub fn get_bitbank_order_id(&self, symbol: &str, hft_order_id: OrderId) -> Option<i64> {
        self.orders
            .get(&RefSymbolOrderId::new(symbol, hft_order_id))?
            .bitbank_order_id
    }

    pub fn update_from_cancel(
        &mut self,
        symbol: &str,
        hft_order_id: OrderId,
        resp: &OrderResponse,
    ) -> Option<Order> {
        let key = RefSymbolOrderId::new(symbol, hft_order_id);
        let order_ext = self.orders.get_mut(&key)?;

        let ts = resp.ordered_at * 1_000_000;
        if ts >= order_ext.order.exch_timestamp {
            order_ext.order.exch_timestamp = ts;
            order_ext.order.status = resp.status;
            order_ext.order.req = Status::None;
            order_ext.order.leaves_qty = resp.remaining_amount;
            order_ext.order.exec_qty = resp.executed_amount;
        }

        self.maybe_remove_and_return(symbol, hft_order_id, false)
    }

    pub fn update_cancel_fail(
        &mut self,
        symbol: &str,
        hft_order_id: OrderId,
        error: &BitbankError,
    ) -> Option<Order> {
        match error {
            BitbankError::OrderError { code: 50010, .. } => {
                // Order not found / already filled or canceled
                let key = RefSymbolOrderId::new(symbol, hft_order_id);
                if let Some(order_ext) = self.orders.get_mut(&key) {
                    order_ext.order.req = Status::None;
                    order_ext.order.status = Status::None;
                }
                self.maybe_remove_and_return(symbol, hft_order_id, false)
            }
            err => {
                error!(?err, "cancel error");
                let key = RefSymbolOrderId::new(symbol, hft_order_id);
                if let Some(order_ext) = self.orders.get_mut(&key) {
                    order_ext.order.req = Status::None;
                }
                None
            }
        }
    }

    pub fn cancel_all_from_rest(&mut self, symbol: &str) -> Vec<Order> {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let mut result = Vec::new();

        for (key, order_ext) in &mut self.orders {
            if key.symbol != symbol {
                continue;
            }
            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
            order_ext.removed_by_rest = true;
            order_ext.order.status = Status::Canceled;
            order_ext.order.exch_timestamp = now;
            if !already_removed {
                result.push(order_ext.order.clone());
            }
        }

        // Clean up fully-removed orders
        self.orders.retain(|_, ext| {
            !(ext.removed_by_ws && ext.removed_by_rest)
        });
        // Clean up reverse map entries for symbol
        self.bitbank_id_map
            .retain(|_, sym_oid| sym_oid.symbol != symbol);

        result
    }

    // ── WebSocket update ─────────────────────────────────────────────────────

    pub fn update_from_ws(&mut self, ws_order: &SpotOrder) -> Result<Option<Order>, BitbankError> {
        let sym_oid = self
            .bitbank_id_map
            .get(&ws_order.order_id)
            .ok_or(BitbankError::OrderNotFound)?;

        let symbol = sym_oid.symbol.clone();
        let hft_order_id = sym_oid.order_id;

        let ref_key = RefSymbolOrderId::new(&symbol, hft_order_id);
        let order_ext = self
            .orders
            .get_mut(&ref_key)
            .ok_or(BitbankError::OrderNotFound)?;

        let ts = ws_order.ordered_at * 1_000_000;
        if ts >= order_ext.order.exch_timestamp {
            order_ext.order.exch_timestamp = ts;
            order_ext.order.status = ws_order.status;
            order_ext.order.qty = ws_order.start_amount;
            order_ext.order.leaves_qty = ws_order.remaining_amount;
            order_ext.order.exec_qty = ws_order.executed_amount;
            order_ext.order.side = ws_order.side;
            order_ext.order.order_type = ws_order.order_type;
            if ws_order.average_price > 0.0 {
                order_ext.order.exec_price_tick =
                    (ws_order.average_price / order_ext.order.tick_size).round() as i64;
            }
        }

        Ok(self.maybe_remove_and_return(&symbol, hft_order_id, true))
    }

    // ── GC ───────────────────────────────────────────────────────────────────

    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ns = now - 300_000_000_000; // 5 min

        // Collect only owned keys — no references to self.orders remain after this.
        let stale_keys: Vec<SymbolOrderId> = self
            .orders
            .iter()
            .filter(|(_, ext)| {
                ext.order.status != Status::New
                    && ext.order.status != Status::PartiallyFilled
                    && ext.order.exch_timestamp < stale_ns
            })
            .map(|(k, _)| SymbolOrderId::new(k.symbol.clone(), k.order_id))
            .collect();

        for key in stale_keys {
            if let Some(ext) =
                self.orders.remove(&RefSymbolOrderId::new(&key.symbol, key.order_id))
            {
                if let Some(bb_id) = ext.bitbank_order_id {
                    self.bitbank_id_map.remove(&bb_id);
                }
            }
        }
    }

    // ── Helper ───────────────────────────────────────────────────────────────

    fn maybe_remove_and_return(
        &mut self,
        symbol: &str,
        hft_order_id: OrderId,
        by_ws: bool,
    ) -> Option<Order> {
        let key = RefSymbolOrderId::new(symbol, hft_order_id);
        let order_ext = self.orders.get_mut(&key)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if by_ws {
            order_ext.removed_by_ws = true;
        } else {
            order_ext.removed_by_rest = true;
        }

        let terminal = order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled;

        let result = if already_removed || !terminal {
            // Publish unless already published for this terminal event
            if already_removed { None } else { Some(order_ext.order.clone()) }
        } else {
            Some(order_ext.order.clone())
        };

        if terminal && order_ext.removed_by_ws && order_ext.removed_by_rest {
            if let Some(bb_id) = order_ext.bitbank_order_id {
                self.bitbank_id_map.remove(&bb_id);
            }
            let owned_key = SymbolOrderId::new(symbol.to_string(), hft_order_id);
            self.orders.remove(&owned_key);
        }

        result
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(key, ext)| {
                symbol.as_ref().map(|s| key.symbol == *s).unwrap_or(true)
                    && ext.order.active()
            })
            .map(|(_, ext)| ext.order.clone())
            .collect()
    }
}
