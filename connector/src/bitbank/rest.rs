use chrono::Utc;
use hftbacktest::types::{OrdType, Side};
use serde::Deserialize;

use crate::{
    bitbank::{
        BitbankError,
        msg::rest::{ApiResponse, AssetsData, OrderResponse},
    },
    utils::sign_hmac_sha256,
};

#[derive(Clone)]
pub struct BitbankClient {
    client: reqwest::Client,
    api_url: String,
    pub api_key: String,
    pub secret: String,
}

impl BitbankClient {
    pub fn new(api_url: &str, api_key: &str, secret: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_url: api_url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
        }
    }

    fn nonce() -> String {
        Utc::now().timestamp_millis().to_string()
    }

    fn sign(&self, nonce: &str, message: &str) -> String {
        sign_hmac_sha256(&self.secret, &format!("{nonce}{message}"))
    }

    // ── Authenticated GET ────────────────────────────────────────────────────

    async fn get_auth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        query: Option<&str>,
    ) -> Result<T, reqwest::Error> {
        let nonce = Self::nonce();
        let signed_msg = match query {
            Some(q) => format!("{path}?{q}"),
            None => path.to_string(),
        };
        let signature = self.sign(&nonce, &signed_msg);

        let url = match query {
            Some(q) => format!("{}{}?{}", self.api_url, path, q),
            None => format!("{}{}", self.api_url, path),
        };

        self.client
            .get(&url)
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-NONCE", &nonce)
            .header("ACCESS-SIGNATURE", &signature)
            .header("Content-Type", "application/json")
            .send()
            .await?
            .json()
            .await
    }

    // ── Authenticated POST ───────────────────────────────────────────────────

    async fn post_auth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: &str,
    ) -> Result<T, reqwest::Error> {
        let nonce = Self::nonce();
        let signature = self.sign(&nonce, body);

        self.client
            .post(format!("{}{}", self.api_url, path))
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-NONCE", &nonce)
            .header("ACCESS-SIGNATURE", &signature)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await?
            .json()
            .await
    }

    // ── Authenticated DELETE ─────────────────────────────────────────────────

    async fn delete_auth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: &str,
    ) -> Result<T, reqwest::Error> {
        let nonce = Self::nonce();
        let signature = self.sign(&nonce, body);

        self.client
            .delete(format!("{}{}", self.api_url, path))
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-NONCE", &nonce)
            .header("ACCESS-SIGNATURE", &signature)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await?
            .json()
            .await
    }

    // ── Public API methods ───────────────────────────────────────────────────

    pub async fn get_assets(&self) -> Result<AssetsData, BitbankError> {
        let resp: ApiResponse<serde_json::Value> =
            self.get_auth("/v1/user/assets", None).await?;
        if resp.success != 1 {
            let code = resp.data.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
            return Err(BitbankError::ApiError { code });
        }
        Ok(serde_json::from_value(resp.data)?)
    }

    pub async fn cancel_all_orders(&self, pair: &str) -> Result<Vec<OrderResponse>, BitbankError> {
        let body = format!(r#"{{"pair":"{pair}"}}"#);
        let resp: ApiResponse<serde_json::Value> =
            self.post_auth("/v1/user/spot/cancel_orders", &body).await?;
        if resp.success != 1 {
            let code = resp.data.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
            return Err(BitbankError::ApiError { code });
        }
        let orders: Vec<OrderResponse> = serde_json::from_value(
            resp.data
                .get("orders")
                .cloned()
                .unwrap_or(serde_json::Value::Array(vec![])),
        )?;
        Ok(orders)
    }

    pub async fn submit_order(
        &self,
        pair: &str,
        side: Side,
        price: f64,
        price_prec: usize,
        qty: f64,
        qty_prec: usize,
        order_type: OrdType,
    ) -> Result<OrderResponse, BitbankError> {
        let side_str = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
            s => return Err(BitbankError::InvalidRequest(format!("invalid side: {s:?}"))),
        };
        let type_str = match order_type {
            OrdType::Limit => "limit",
            OrdType::Market => "market",
            t => return Err(BitbankError::InvalidRequest(format!("unsupported order type: {t:?}"))),
        };

        let body = if order_type == OrdType::Market {
            format!(
                r#"{{"pair":"{pair}","amount":"{qty:.qty_prec$}","side":"{side_str}","type":"{type_str}"}}"#
            )
        } else {
            format!(
                r#"{{"pair":"{pair}","amount":"{qty:.qty_prec$}","price":"{price:.price_prec$}","side":"{side_str}","type":"{type_str}"}}"#
            )
        };

        let resp: ApiResponse<serde_json::Value> =
            self.post_auth("/v1/user/spot/order", &body).await?;
        if resp.success != 1 {
            let code = resp.data.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
            return Err(BitbankError::OrderError {
                code,
                msg: format!("submit order failed for {pair}"),
            });
        }
        Ok(serde_json::from_value(resp.data)?)
    }

    pub async fn cancel_order(
        &self,
        pair: &str,
        bitbank_order_id: i64,
    ) -> Result<OrderResponse, BitbankError> {
        let body = format!(r#"{{"pair":"{pair}","order_id":{bitbank_order_id}}}"#);
        let resp: ApiResponse<serde_json::Value> =
            self.delete_auth("/v1/user/spot/order", &body).await?;
        if resp.success != 1 {
            let code = resp.data.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
            return Err(BitbankError::OrderError {
                code,
                msg: format!("cancel order {bitbank_order_id} failed for {pair}"),
            });
        }
        Ok(serde_json::from_value(resp.data)?)
    }
}
