//! Polymarket's market websocket: connection, commands, and decoding its messages
//! into exchange-neutral events.

use std::collections::BTreeSet;

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    Side,
    event::{Event, Level, Snapshot, Update},
    exchange::{Operation, Socket},
    fixed,
};

const URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
// Application-level heartbeat; the server answers with PONG.
pub const PING: &str = "PING";
pub const PONG: &str = "PONG";

pub async fn connect() -> anyhow::Result<Socket> {
    let (socket, _) = connect_async(URL).await?;
    Ok(socket)
}

pub fn subscribe(assets_ids: &BTreeSet<String>) -> Message {
    Message::text(serde_json::json!({ "assets_ids": assets_ids, "type": "market" }).to_string())
}

pub fn update(operation: Operation, asset_id: &str) -> Message {
    let operation = match operation {
        Operation::Subscribe => "subscribe",
        Operation::Unsubscribe => "unsubscribe",
    };
    Message::text(
        serde_json::json!({ "operation": operation, "assets_ids": [asset_id] }).to_string(),
    )
}

pub fn decode(frame: &str) -> Vec<anyhow::Result<Event>> {
    parse(frame)
        .into_iter()
        .flat_map(|message| match message {
            Ok(WireEvent::Book(book)) => vec![snapshot(book)],
            Ok(WireEvent::PriceChange(change)) => updates(change),
            Ok(WireEvent::Other) => Vec::new(),
            Err(error) => vec![Err(error.into())],
        })
        .collect()
}

// The initial books of a subscription arrive as one array frame. Its elements are
// decoded one by one so that a single bad element does not cost the other books.
fn parse(frame: &str) -> Vec<serde_json::Result<WireEvent>> {
    if !frame.starts_with('[') {
        return vec![serde_json::from_str(frame)];
    }
    match serde_json::from_str::<Vec<&RawValue>>(frame) {
        Ok(elements) => elements
            .into_iter()
            .map(|element| serde_json::from_str(element.get()))
            .collect(),
        Err(error) => vec![Err(error)],
    }
}

fn snapshot(book: Book) -> anyhow::Result<Event> {
    Ok(Event::Snapshot(Snapshot {
        asset: book.asset_id,
        exchange_ts: timestamp(&book.timestamp)?,
        bids: levels(&book.bids)?,
        asks: levels(&book.asks)?,
    }))
}

fn updates(change: PriceChange) -> Vec<anyhow::Result<Event>> {
    let exchange_ts = match timestamp(&change.timestamp) {
        Ok(exchange_ts) => exchange_ts,
        Err(error) => return vec![Err(error)],
    };
    change
        .price_changes
        .into_iter()
        .map(|line| {
            Ok(Event::Update(Update {
                asset: line.asset_id,
                exchange_ts,
                side: line.side.into(),
                price: fixed::parse(&line.price)?,
                quantity: fixed::parse(&line.size)?,
            }))
        })
        .collect()
}

fn levels(levels: &[WireLevel]) -> anyhow::Result<Vec<Level>> {
    levels
        .iter()
        .map(|level| {
            Ok(Level {
                price: fixed::parse(&level.price)?,
                quantity: fixed::parse(&level.size)?,
            })
        })
        .collect()
}

fn timestamp(millis: &str) -> anyhow::Result<DateTime<Utc>> {
    let millis: i64 = millis
        .parse()
        .with_context(|| format!("invalid timestamp: {millis}"))?;
    DateTime::from_timestamp_millis(millis)
        .with_context(|| format!("timestamp out of range: {millis}"))
}

#[derive(Deserialize, Debug)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum WireEvent {
    Book(Book),
    PriceChange(PriceChange),
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
struct Book {
    asset_id: String,
    timestamp: String,
    bids: Vec<WireLevel>,
    asks: Vec<WireLevel>,
}

#[derive(Deserialize, Debug)]
struct WireLevel {
    price: String,
    size: String,
}

#[derive(Deserialize, Debug)]
struct PriceChange {
    timestamp: String,
    price_changes: Vec<LineChange>,
}

#[derive(Deserialize, Debug)]
struct LineChange {
    asset_id: String,
    price: String,
    size: String,
    side: OrderSide,
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
enum OrderSide {
    Buy,
    Sell,
}

impl From<OrderSide> for Side {
    fn from(side: OrderSide) -> Self {
        match side {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YES: &str =
        "90227223115596293448966158151606153337847900483048338623540402385929720677904";
    const NO: &str =
        "105134691322323565615831879054043168027913414000042547943201361115853277749482";

    fn book_frame(asset_id: &str) -> String {
        format!(
            r#"{{"market": "0xe50c71cc5e2372bf0f6194a02e51459dd03b2c7b2d78a0d4650e5f36c425338b",
                 "asset_id": "{asset_id}", "timestamp": "1793977813000",
                 "hash": "608a04e09e2a3e8b514160d1f891619cd2cd20e5",
                 "bids": [{{"price": "0.997", "size": "105486.43"}}, {{"price": "0.998", "size": "287197.36"}}],
                 "asks": [{{"price": "0.999", "size": "1369896.93"}}],
                 "tick_size": "0.001", "event_type": "book", "last_trade_price": "0.001"}}"#
        )
    }

    fn price_change_frame() -> String {
        format!(
            r#"{{"market": "0x61d9486c0f7e14ed98f3b177b6adcb3cd45646c92e8bbfbf209789b86472d4b6",
                 "price_changes": [
                   {{"asset_id": "{YES}", "price": "0.001", "size": "375524.83", "side": "BUY",
                     "hash": "2634bcad884ee13ca02ab150925aca28178c1f0f", "best_bid": "0.02", "best_ask": "0.023"}},
                   {{"asset_id": "{NO}", "price": "0.999", "size": "375524.83", "side": "SELL",
                     "hash": "3245c13d127b56144513277772f7dccd602648b8", "best_bid": "0.977", "best_ask": "0.98"}}
                 ],
                 "timestamp": "1793977816442", "event_type": "price_change"}}"#
        )
    }

    #[test]
    fn subscribe_message_has_documented_shape() {
        let assets = BTreeSet::from([YES.to_owned(), NO.to_owned()]);

        let message = subscribe(&assets).into_text().unwrap();
        let json: serde_json::Value = serde_json::from_str(message.as_str()).unwrap();

        assert_eq!(
            json,
            serde_json::json!({ "assets_ids": [NO, YES], "type": "market" })
        );
    }

    #[test]
    fn update_message_has_documented_shape() {
        let message = update(Operation::Unsubscribe, YES).into_text().unwrap();
        let json: serde_json::Value = serde_json::from_str(message.as_str()).unwrap();

        assert_eq!(
            json,
            serde_json::json!({ "operation": "unsubscribe", "assets_ids": [YES] })
        );
    }

    #[test]
    fn book_decodes_to_a_snapshot() {
        let decoded = decode(&book_frame(YES));

        let [Ok(Event::Snapshot(snapshot))] = decoded.as_slice() else {
            panic!("expected one snapshot");
        };
        assert_eq!(snapshot.asset, YES);
        assert_eq!(snapshot.exchange_ts.timestamp_millis(), 1_793_977_813_000);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.bids[1].price, 998_000);
        assert_eq!(snapshot.bids[1].quantity, 287_197_360_000);
        assert_eq!(snapshot.asks[0].price, 999_000);
    }

    #[test]
    fn price_change_decodes_to_one_update_per_line() {
        let decoded = decode(&price_change_frame());

        let [Ok(Event::Update(yes)), Ok(Event::Update(no))] = decoded.as_slice() else {
            panic!("expected two updates");
        };
        assert_eq!(yes.asset, YES);
        assert_eq!(yes.exchange_ts.timestamp_millis(), 1_793_977_816_442);
        assert_eq!(yes.side, Side::Buy);
        assert_eq!(yes.price, 1_000);
        assert_eq!(yes.quantity, 375_524_830_000);
        assert_eq!(no.asset, NO);
        assert_eq!(no.side, Side::Sell);
        assert_eq!(no.price, 999_000);
    }

    #[test]
    fn frame_may_hold_one_event_or_a_batch() {
        let batch = format!("[{}, {}]", book_frame(YES), book_frame(NO));

        assert_eq!(decode(&book_frame(YES)).len(), 1);
        assert_eq!(decode(&batch).len(), 2);
    }

    #[test]
    fn bad_element_does_not_cost_the_rest_of_the_batch() {
        let batch = format!(
            r#"[{}, {{"event_type": "book", "asset_id": 5}}, {}]"#,
            book_frame(YES),
            book_frame(NO)
        );

        let decoded = decode(&batch);

        assert_eq!(decoded.len(), 3);
        assert!(decoded[0].is_ok());
        assert!(decoded[1].is_err());
        assert!(decoded[2].is_ok());
    }

    #[test]
    fn unknown_event_types_decode_to_nothing() {
        let frame = r#"{"event_type": "tick_size_change", "asset_id": "1", "old_tick_size": "0.01", "new_tick_size": "0.001"}"#;

        assert!(decode(frame).is_empty());
    }

    #[test]
    fn unreadable_numbers_are_errors() {
        let frame = book_frame(YES).replace("0.997", "abc");

        assert!(decode(&frame)[0].is_err());
    }
}
