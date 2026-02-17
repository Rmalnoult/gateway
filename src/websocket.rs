//! Websocket server

use std::{collections::HashMap, ops::Neg, sync::Arc};

use drift_rs::{
    constants::ProgramData,
    event_subscriber::{DriftEvent, EventSubscriber, PubsubClient},
    types::{MarketType, Order, OrderType, PositionDirection},
    Pubkey, Wallet,
};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{write_response, ErrorResponse, Request, Response},
        http::{HeaderValue, Method as HttpMethod, Response as HttpResponse, StatusCode},
        Message,
    },
};

use crate::{
    types::{get_market_decimals, Market, PRICE_DECIMALS},
    LOG_TARGET,
};

/// Start the websocket server
pub async fn start_ws_server(
    listen_address: &str,
    ws_client: Arc<PubsubClient>,
    wallet: Arc<Wallet>,
    program_data: &'static ProgramData,
) {
    // Create the event loop and TCP listener we'll accept connections on.
    let listener = TcpListener::bind(&listen_address)
        .await
        .expect("failed to bind");
    info!("Ws server listening at: ws://{}", listen_address);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(accept_connection(
                stream,
                Arc::clone(&ws_client),
                Arc::clone(&wallet),
                program_data,
            ));
        }
    });
}

fn build_ws_handshake_error(status: StatusCode, reason: String) -> ErrorResponse {
    let mut builder = HttpResponse::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8");
    if let Some(headers) = builder.headers_mut() {
        headers.insert("connection", HeaderValue::from_static("close"));
    }
    builder
        .body(Some(reason))
        .unwrap_or_else(|_| ErrorResponse::new(Some("websocket handshake failed".to_string())))
}

fn handle_ws_handshake(
    request: &Request,
    mut response: Response,
) -> Result<Response, ErrorResponse> {
    if request.method() != HttpMethod::GET {
        return Err(build_ws_handshake_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "websocket endpoint requires GET".to_string(),
        ));
    }

    let path = request.uri().path();
    if path != "/" && path != "/ws" {
        return Err(build_ws_handshake_error(
            StatusCode::NOT_FOUND,
            format!("unsupported websocket path: {path}"),
        ));
    }

    response
        .headers_mut()
        .insert("server", HeaderValue::from_static("gateway"));
    Ok(response)
}

fn serialize_http_error_response(status: StatusCode, reason: &str) -> Option<Vec<u8>> {
    let response = build_ws_handshake_error(status, reason.to_string());
    let mut output = Vec::new();
    if write_response(&mut output, &response).is_err() {
        return None;
    }
    if let Some(body) = response.body() {
        output.extend_from_slice(body.as_bytes());
    }
    Some(output)
}

async fn maybe_write_http_handshake_error(stream: &mut TcpStream) -> bool {
    let mut buf = [0_u8; 4096];
    let read = match stream.peek(&mut buf).await {
        Ok(n) => n,
        Err(_) => return false,
    };
    if read == 0 {
        return true;
    }

    let request = String::from_utf8_lossy(&buf[..read]);
    // Only attempt HTTP error responses when we have a full header.
    if !request.contains("\r\n\r\n") {
        return false;
    }

    let mut lines = request.split("\r\n");
    let request_line = match lines.next() {
        Some(line) => line,
        None => return false,
    };

    let mut parts = request_line.split_whitespace();
    let method = match parts.next() {
        Some(m) => m,
        None => return false,
    };
    let path = match parts.next() {
        Some(p) => p,
        None => return false,
    };

    // Ignore non-HTTP traffic.
    if !request_line.contains("HTTP/") {
        return false;
    }

    let lower = request.to_ascii_lowercase();
    let status_reason = if method != "GET" {
        Some((
            StatusCode::METHOD_NOT_ALLOWED,
            "websocket endpoint requires GET",
        ))
    } else if path != "/" && path != "/ws" {
        Some((StatusCode::NOT_FOUND, "unsupported websocket path"))
    } else if !lower.contains("\r\nupgrade: websocket\r\n") {
        Some((
            StatusCode::UPGRADE_REQUIRED,
            "missing Upgrade: websocket header",
        ))
    } else if !lower.contains("connection: upgrade")
        && !lower.contains("connection: keep-alive, upgrade")
    {
        Some((
            StatusCode::UPGRADE_REQUIRED,
            "missing Connection: Upgrade header",
        ))
    } else if !lower.contains("\r\nsec-websocket-key:") {
        Some((StatusCode::BAD_REQUEST, "missing Sec-WebSocket-Key header"))
    } else if !lower.contains("\r\nsec-websocket-version: 13\r\n") {
        Some((
            StatusCode::UPGRADE_REQUIRED,
            "unsupported Sec-WebSocket-Version (expected 13)",
        ))
    } else {
        None
    };

    if let Some((status, reason)) = status_reason {
        if let Some(response) = serialize_http_error_response(status, reason) {
            let _ = stream.write_all(&response).await;
        }
        let _ = stream.shutdown().await;
        return true;
    }

    false
}

async fn accept_connection(
    mut stream: TcpStream,
    ws_client: Arc<PubsubClient>,
    wallet: Arc<Wallet>,
    program_data: &'static ProgramData,
) {
    let addr = stream
        .peer_addr()
        .map(|peer| peer.to_string())
        .unwrap_or_else(|_| "<unknown-peer>".to_string());
    if maybe_write_http_handshake_error(&mut stream).await {
        log::warn!(target: LOG_TARGET, "rejected invalid Ws handshake: {addr}");
        return;
    }
    // Returning Err(ErrorResponse) from the callback writes the HTTP error to this stream.
    let ws_stream = match accept_hdr_async(stream, handle_ws_handshake).await {
        Ok(ws) => ws,
        Err(err) => {
            log::error!(target: LOG_TARGET, "Ws connection failed: {addr}, err={err}");
            return;
        }
    };
    info!(target: LOG_TARGET, "accepted Ws connection: {}", addr);

    let (mut ws_out, mut ws_in) = ws_stream.split();
    let (message_tx, mut message_rx) = tokio::sync::mpsc::channel::<Message>(64);
    let subscriptions = Arc::new(Mutex::new(HashMap::<u8, JoinHandle<()>>::default()));

    // writes messages to the connection
    let addr_for_writer = addr.clone();
    tokio::spawn(async move {
        while let Some(msg) = message_rx.recv().await {
            if msg.is_close() {
                let _ = ws_out.send(msg).await;
                let _ = ws_out.close().await;
                debug!(target: LOG_TARGET, "closing Ws connection (send half): {addr_for_writer}");
                break;
            }
            ws_out.send(msg).await.expect("sent");
        }
    });

    // watches incoming messages from the connection
    while let Some(Ok(msg)) = ws_in.next().await {
        match msg {
            Message::Text(ref request) => match serde_json::from_str::<'_, WsRequest>(request) {
                Ok(request) => {
                    match request.method {
                        Method::Subscribe => {
                            // TODO: support subscriptions for individual channels and/or markets
                            let mut subscription_map = subscriptions.lock().await;
                            if subscription_map.contains_key(&request.sub_account_id) {
                                info!(target: LOG_TARGET, "subscription already exists for: {}", request.sub_account_id);
                                message_tx
                                    .send(Message::text(
                                        json!({
                                            "error": "bad request",
                                            "reason": "subscription already exists",
                                        })
                                        .to_string(),
                                    ))
                                    .await
                                    .unwrap();
                                continue;
                            }
                            info!(target: LOG_TARGET, "subscribing to events for: {}", request.sub_account_id);
                            let addr_for_subscription = addr.clone();
                            let join_handle = tokio::spawn({
                                let ws_client_ref = Arc::clone(&ws_client);
                                let sub_account_address =
                                    wallet.sub_account(request.sub_account_id as u16);
                                let subscription_map = Arc::clone(&subscriptions);
                                let sub_account_id = request.sub_account_id;
                                let message_tx = message_tx.clone();
                                let addr_for_subscription = addr_for_subscription.clone();

                                async move {
                                    loop {
                                        let mut event_stream = match EventSubscriber::subscribe(
                                            Arc::clone(&ws_client_ref),
                                            sub_account_address,
                                        )
                                        .await
                                        {
                                            Ok(stream) => stream,
                                            Err(err) => {
                                                log::error!(target: LOG_TARGET, "event subscribe failed: {sub_account_id:?}, {err:?}");
                                                break;
                                            }
                                        };

                                        debug!(target: LOG_TARGET, "event stream connected: {sub_account_id:?}");
                                        while let Some(ref update) = event_stream.next().await {
                                            let (channel, data) = map_drift_event_for_account(
                                                program_data,
                                                update,
                                                sub_account_address,
                                            );
                                            if data.is_none() {
                                                continue;
                                            }
                                            if message_tx
                                                .send(Message::text(
                                                    serde_json::to_string(&WsEvent {
                                                        data,
                                                        channel,
                                                        sub_account_id,
                                                    })
                                                    .expect("serializes"),
                                                ))
                                                .await
                                                .is_err()
                                            {
                                                warn!(target: LOG_TARGET, "failed sending Ws message: {}", addr_for_subscription);
                                                break;
                                            }
                                        }
                                    }
                                    warn!(target: LOG_TARGET, "event stream finished: {sub_account_id:?}");
                                    subscription_map.lock().await.remove(&sub_account_id);
                                    // the event subscription task has failed
                                    // close the Ws so client can handle resubscription
                                    let _ = message_tx.try_send(Message::Close(None));
                                }
                            });
                            subscription_map.insert(request.sub_account_id, join_handle);
                        }
                        Method::Unsubscribe => {
                            info!(target: LOG_TARGET, "unsubscribing events of: {}", request.sub_account_id);
                            // TODO: support ending by channel, this ends all channels
                            let mut subscription_map = subscriptions.lock().await;
                            if let Some(task) = subscription_map.remove(&request.sub_account_id) {
                                task.abort();
                            }
                        }
                    }
                }
                Err(err) => {
                    message_tx
                        .send(Message::text(
                            json!({
                                "error": "bad request",
                                "reason": err.to_string(),
                            })
                            .to_string(),
                        ))
                        .await
                        .unwrap();
                }
            },
            Message::Close(frame) => {
                info!(target: LOG_TARGET, "received Ws close: {}", addr);
                let _ = message_tx.send(Message::Close(frame)).await;
                break;
            }
            // tokio-tungstenite handles ping/pong transparently
            _ => (),
        }
    }

    let subs = subscriptions.lock().await;
    for (_k, task) in subs.iter() {
        task.abort();
    }
    info!(target: LOG_TARGET, "closing Ws connection: {}", addr);
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Method {
    Subscribe,
    Unsubscribe,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Channel {
    Fills,
    Orders,
    Funding,
    Swap,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct WsRequest {
    method: Method,
    sub_account_id: u8,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct WsEvent<T: Serialize> {
    data: T,
    channel: Channel,
    sub_account_id: u8,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AccountEvent {
    #[serde(rename_all = "camelCase")]
    Fill {
        side: Side,
        fee: Decimal,
        amount: Decimal,
        price: Decimal,
        oracle_price: Decimal,
        order_id: u32,
        market_index: u16,
        #[serde(
            serialize_with = "crate::types::ser_market_type",
            deserialize_with = "crate::types::de_market_type"
        )]
        market_type: MarketType,
        ts: u64,

        /// The index of the event in the transaction
        tx_idx: usize,
        signature: String,

        maker: Option<String>,
        maker_order_id: Option<u32>,
        maker_fee: Option<Decimal>,
        taker: Option<String>,
        taker_order_id: Option<u32>,
        taker_fee: Option<Decimal>,
    },
    #[serde(rename_all = "camelCase")]
    OrderCreate {
        order: OrderWithDecimals,
        ts: u64,
        signature: String,
        tx_idx: usize,
    },
    #[serde(rename_all = "camelCase")]
    OrderCancel {
        order_id: u32,
        ts: u64,
        signature: String,
        tx_idx: usize,
    },
    #[serde(rename_all = "camelCase")]
    OrderCancelMissing {
        user_order_id: u8,
        order_id: u32,
        signature: String,
    },
    #[serde(rename_all = "camelCase")]
    OrderExpire {
        order_id: u32,
        fee: Decimal,
        ts: u64,
        signature: String,
    },
    #[serde(rename_all = "camelCase")]
    FundingPayment {
        amount: Decimal,
        market_index: u16,
        ts: u64,
        signature: String,
        tx_idx: usize,
    },
    #[serde(rename_all = "camelCase")]
    Swap {
        amount_in: Decimal,
        amount_out: Decimal,
        market_in: u16,
        market_out: u16,
        ts: u64,
        tx_idx: usize,
        signature: String,
    },
    #[serde(rename_all = "camelCase")]
    Trigger { order_id: u32, oracle_price: u64 },
}

impl AccountEvent {
    fn fill(
        side: PositionDirection,
        fee: i64,
        base_amount: u64,
        quote_amount: u64,
        oracle_price: i64,
        order_id: u32,
        ts: u64,
        decimals: u32,
        signature: &String,
        tx_idx: usize,
        market_index: u16,
        market_type: MarketType,
        maker: Option<String>,
        maker_order_id: Option<u32>,
        maker_fee: Option<i64>,
        taker: Option<String>,
        taker_order_id: Option<u32>,
        taker_fee: Option<i64>,
    ) -> Self {
        let base_amount = Decimal::new(base_amount as i64, decimals);
        let price = Decimal::new(quote_amount as i64, PRICE_DECIMALS) / base_amount;
        AccountEvent::Fill {
            side: if let PositionDirection::Long = side {
                Side::Buy
            } else {
                Side::Sell
            },
            price: price.normalize(),
            oracle_price: Decimal::new(oracle_price, PRICE_DECIMALS).normalize(),
            fee: Decimal::new(fee, PRICE_DECIMALS).normalize(),
            order_id,
            amount: base_amount.normalize(),
            ts,
            signature: signature.to_string(),
            market_index,
            market_type,
            tx_idx,
            maker,
            maker_order_id,
            maker_fee: maker_fee.map(|x| Decimal::new(x, PRICE_DECIMALS)),
            taker,
            taker_order_id,
            taker_fee: taker_fee.map(|x| Decimal::new(x, PRICE_DECIMALS)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Side {
    Buy,
    Sell,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderWithDecimals {
    /// The slot the order was placed
    pub slot: u64,
    /// The limit price for the order (can be 0 for market orders)
    /// For orders with an auction, this price isn't used until the auction is complete
    pub price: Decimal,
    /// The size of the order
    pub amount: Decimal,
    /// The amount of the order filled
    pub filled: Decimal,
    /// At what price the order will be triggered. Only relevant for trigger orders
    pub trigger_price: Decimal,
    /// The start price for the auction. Only relevant for market/oracle orders
    pub auction_start_price: Decimal,
    /// The end price for the auction. Only relevant for market/oracle orders
    pub auction_end_price: Decimal,
    /// The time when the order will expire
    pub max_ts: i64,
    /// If set, the order limit price is the oracle price + this offset
    pub oracle_price_offset: Decimal,
    /// The id for the order. Each users has their own order id space
    pub order_id: u32,
    /// The perp/spot market index
    pub market_index: u16,
    /// The type of order
    #[serde(serialize_with = "ser_order_type", deserialize_with = "de_order_type")]
    pub order_type: OrderType,
    /// Whether market is spot or perp
    #[serde(
        serialize_with = "crate::types::ser_market_type",
        deserialize_with = "crate::types::de_market_type"
    )]
    pub market_type: MarketType,
    /// User generated order id. Can make it easier to place/cancel orders
    pub user_order_id: u8,
    #[serde(
        serialize_with = "ser_position_direction",
        deserialize_with = "de_position_direction"
    )]
    pub direction: PositionDirection,
    /// Whether the order is allowed to only reduce position size
    pub reduce_only: bool,
    /// Whether the order must be a maker
    pub post_only: bool,
    /// Whether the order must be canceled the same slot it is placed
    pub immediate_or_cancel: bool,
    /// How many slots the auction lasts
    pub auction_duration: u8,
}

impl OrderWithDecimals {
    fn from_order(value: Order, decimals: u32) -> Self {
        Self {
            slot: value.slot,
            price: Decimal::new(value.price as i64, PRICE_DECIMALS).normalize(),
            amount: Decimal::new(value.base_asset_amount as i64, decimals).normalize(),
            filled: Decimal::new(value.base_asset_amount_filled as i64, decimals).normalize(),
            trigger_price: Decimal::new(value.trigger_price as i64, PRICE_DECIMALS).normalize(),
            auction_start_price: Decimal::new(value.auction_start_price, PRICE_DECIMALS)
                .normalize(),
            auction_end_price: Decimal::new(value.auction_end_price, PRICE_DECIMALS).normalize(),
            oracle_price_offset: Decimal::new(value.oracle_price_offset as i64, PRICE_DECIMALS)
                .normalize(),
            max_ts: value.max_ts,
            order_id: value.order_id,
            market_index: value.market_index,
            order_type: value.order_type,
            market_type: value.market_type,
            user_order_id: value.user_order_id,
            direction: value.direction,
            reduce_only: value.reduce_only,
            post_only: value.post_only,
            immediate_or_cancel: value.immediate_or_cancel,
            auction_duration: value.auction_duration,
        }
    }
}

fn ser_order_type<S>(x: &OrderType, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(match x {
        OrderType::Limit => "limit",
        OrderType::Market => "market",
        OrderType::Oracle => "oracle",
        OrderType::TriggerLimit => "triggerLimit",
        OrderType::TriggerMarket => "triggerMarket",
    })
}

fn de_order_type<'de, D>(deserializer: D) -> Result<OrderType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "limit" => Ok(OrderType::Limit),
        "market" => Ok(OrderType::Market),
        "oracle" => Ok(OrderType::Oracle),
        "triggerLimit" => Ok(OrderType::TriggerLimit),
        "triggerMarket" => Ok(OrderType::TriggerMarket),
        _ => Err(serde::de::Error::custom(format!(
            "unknown order type: {}",
            s
        ))),
    }
}

fn ser_position_direction<S>(x: &PositionDirection, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(match x {
        PositionDirection::Long => "buy",
        PositionDirection::Short => "sell",
    })
}

fn de_position_direction<'de, D>(deserializer: D) -> Result<PositionDirection, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "buy" => Ok(PositionDirection::Long),
        "sell" => Ok(PositionDirection::Short),
        _ => Err(serde::de::Error::custom(format!(
            "unknown position direction: {}",
            s
        ))),
    }
}

/// Map drift-program events into gateway friendly types for events to the specific UserAccount
pub(crate) fn map_drift_event_for_account(
    program_data: &ProgramData,
    event: &DriftEvent,
    sub_account_address: Pubkey,
) -> (Channel, Option<AccountEvent>) {
    match event {
        DriftEvent::OrderTrigger {
            user: _,
            order_id,
            oracle_price,
            amount: _,
        } => (
            Channel::Orders,
            Some(AccountEvent::Trigger {
                order_id: *order_id,
                oracle_price: *oracle_price,
            }),
        ),
        DriftEvent::OrderFill {
            maker,
            maker_fee,
            maker_order_id,
            maker_side,
            taker,
            taker_fee,
            taker_order_id,
            taker_side,
            base_asset_amount_filled,
            quote_asset_amount_filled,
            oracle_price,
            market_index,
            market_type,
            signature,
            tx_idx,
            ts,
            bit_flags: _,
        } => {
            let decimals =
                get_market_decimals(program_data, Market::new(*market_index, *market_type));
            let fill = if *maker == Some(sub_account_address) {
                Some(AccountEvent::fill(
                    maker_side.unwrap(),
                    *maker_fee,
                    *base_asset_amount_filled,
                    *quote_asset_amount_filled,
                    *oracle_price,
                    *maker_order_id,
                    *ts,
                    decimals,
                    signature,
                    *tx_idx,
                    *market_index,
                    *market_type,
                    (*maker).map(|x| x.to_string()),
                    Some(*maker_order_id),
                    Some(*maker_fee),
                    (*taker).map(|x| x.to_string()),
                    Some(*taker_order_id),
                    Some(*taker_fee as i64),
                ))
            } else if *taker == Some(sub_account_address) {
                Some(AccountEvent::fill(
                    taker_side.unwrap(),
                    (*taker_fee) as i64,
                    *base_asset_amount_filled,
                    *quote_asset_amount_filled,
                    *oracle_price,
                    *taker_order_id,
                    *ts,
                    decimals,
                    signature,
                    *tx_idx,
                    *market_index,
                    *market_type,
                    (*maker).map(|x| x.to_string()),
                    Some(*maker_order_id),
                    Some(*maker_fee),
                    (*taker).map(|x| x.to_string()),
                    Some(*taker_order_id),
                    Some(*taker_fee as i64),
                ))
            } else {
                None
            };

            (Channel::Fills, fill)
        }
        DriftEvent::OrderCancel {
            taker: _,
            maker,
            taker_order_id,
            maker_order_id,
            signature,
            tx_idx,
            ts,
        } => {
            let order_id = if *maker == Some(sub_account_address) {
                maker_order_id
            } else {
                taker_order_id
            };
            (
                Channel::Orders,
                Some(AccountEvent::OrderCancel {
                    order_id: *order_id,
                    ts: *ts,
                    signature: signature.clone(),
                    tx_idx: *tx_idx,
                }),
            )
        }
        DriftEvent::OrderCancelMissing {
            order_id,
            user_order_id,
            signature,
        } => (
            Channel::Orders,
            Some(AccountEvent::OrderCancelMissing {
                user_order_id: *user_order_id,
                order_id: *order_id,
                signature: signature.clone(),
            }),
        ),
        DriftEvent::OrderExpire {
            order_id,
            fee,
            ts,
            signature,
            ..
        } => (
            Channel::Orders,
            Some(AccountEvent::OrderExpire {
                order_id: *order_id,
                fee: Decimal::new((*fee as i64).neg(), PRICE_DECIMALS),
                ts: *ts,
                signature: signature.to_string(),
            }),
        ),
        DriftEvent::OrderCreate {
            order,
            ts,
            signature,
            tx_idx,
            ..
        } => {
            let decimals = get_market_decimals(
                program_data,
                Market::new(order.market_index, order.market_type),
            );
            (
                Channel::Orders,
                Some(AccountEvent::OrderCreate {
                    order: OrderWithDecimals::from_order(*order, decimals),
                    ts: *ts,
                    signature: signature.clone(),
                    tx_idx: *tx_idx,
                }),
            )
        }
        DriftEvent::FundingPayment {
            amount,
            market_index,
            ts,
            tx_idx,
            signature,
            ..
        } => (
            Channel::Funding,
            Some(AccountEvent::FundingPayment {
                amount: Decimal::new(*amount, PRICE_DECIMALS).normalize(),
                market_index: *market_index,
                ts: *ts,
                signature: signature.clone(),
                tx_idx: *tx_idx,
            }),
        ),
        DriftEvent::Swap {
            user,
            amount_in,
            amount_out,
            market_in,
            market_out,
            fee,
            ts,
            signature,
            tx_idx,
        } => {
            let decimals_in = get_market_decimals(program_data, Market::spot(*market_in));
            let decimals_out = get_market_decimals(program_data, Market::spot(*market_out));
            (
                Channel::Swap,
                Some(AccountEvent::Swap {
                    amount_in: Decimal::new(*amount_in as i64, decimals_in),
                    amount_out: Decimal::new(*amount_out as i64, decimals_out),
                    market_in: *market_in,
                    market_out: *market_out,
                    ts: *ts,
                    tx_idx: *tx_idx,
                    signature: signature.clone(),
                }),
            )
        }
    }
}
