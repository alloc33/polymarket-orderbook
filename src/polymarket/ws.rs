//! Polymarket's market websocket: connection, commands and message shapes.

use std::{collections::BTreeSet, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

const URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
// Application-level heartbeat; the server answers with PONG.
pub const PING: &str = "PING";
pub const PONG: &str = "PONG";
pub const PING_INTERVAL: Duration = Duration::from_secs(10);

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect() -> anyhow::Result<Socket> {
    let (socket, _) = connect_async(URL).await?;
    Ok(socket)
}

pub fn subscribe(assets_ids: &BTreeSet<String>) -> Message {
    Message::text(serde_json::json!({ "assets_ids": assets_ids, "type": "market" }).to_string())
}

pub fn update(operation: Operation, asset_id: &str) -> Message {
    Message::text(
        serde_json::json!({ "operation": operation, "assets_ids": [asset_id] }).to_string(),
    )
}

#[derive(Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Subscribe,
    Unsubscribe,
}

// The initial books of a subscription arrive as one array frame. Its elements are
// decoded one by one so that a single bad element does not cost the other books.
pub fn parse(frame: &str) -> Vec<serde_json::Result<Event>> {
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

#[derive(Deserialize, Debug)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum Event {
    Book(Book),
    PriceChange(PriceChange),
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
pub struct Book {
    pub asset_id: String,
    pub timestamp: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Deserialize, Debug)]
pub struct Level {
    pub price: String,
    pub size: String,
}

#[derive(Deserialize, Debug)]
pub struct PriceChange {
    pub timestamp: String,
    pub price_changes: Vec<LevelChange>,
}

#[derive(Deserialize, Debug)]
pub struct LevelChange {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: OrderSide,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
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
    fn book_event_parses() {
        let Event::Book(book) = serde_json::from_str(&book_frame(YES)).unwrap() else {
            panic!("expected a book event");
        };

        assert_eq!(book.asset_id, YES);
        assert_eq!(book.timestamp, "1793977813000");
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.bids[1].price, "0.998");
        assert_eq!(book.bids[1].size, "287197.36");
        assert_eq!(book.asks.len(), 1);
    }

    #[test]
    fn price_change_event_parses() {
        let frame = format!(
            r#"{{"market": "0x61d9486c0f7e14ed98f3b177b6adcb3cd45646c92e8bbfbf209789b86472d4b6",
                 "price_changes": [
                   {{"asset_id": "{YES}", "price": "0.001", "size": "375524.83", "side": "BUY",
                     "hash": "2634bcad884ee13ca02ab150925aca28178c1f0f", "best_bid": "0.02", "best_ask": "0.023"}},
                   {{"asset_id": "{NO}", "price": "0.999", "size": "375524.83", "side": "SELL",
                     "hash": "3245c13d127b56144513277772f7dccd602648b8", "best_bid": "0.977", "best_ask": "0.98"}}
                 ],
                 "timestamp": "1793977816442", "event_type": "price_change"}}"#
        );

        let Event::PriceChange(change) = serde_json::from_str(&frame).unwrap() else {
            panic!("expected a price change event");
        };

        assert_eq!(change.timestamp, "1793977816442");
        assert_eq!(change.price_changes.len(), 2);
        assert_eq!(change.price_changes[0].asset_id, YES);
        assert_eq!(change.price_changes[0].side, OrderSide::Buy);
        assert_eq!(change.price_changes[0].size, "375524.83");
        assert_eq!(change.price_changes[1].side, OrderSide::Sell);
        assert_eq!(change.price_changes[1].price, "0.999");
    }

    #[test]
    fn frame_may_hold_one_event_or_a_batch() {
        let one = book_frame(YES);
        let batch = format!("[{}, {}]", book_frame(YES), book_frame(NO));

        assert_eq!(parse(&one).len(), 1);
        assert_eq!(parse(&batch).len(), 2);
    }

    #[test]
    fn bad_element_does_not_cost_the_rest_of_the_batch() {
        let batch = format!(
            r#"[{}, {{"event_type": "book", "asset_id": 5}}, {}]"#,
            book_frame(YES),
            book_frame(NO)
        );

        let parsed = parse(&batch);

        assert_eq!(parsed.len(), 3);
        assert!(parsed[0].is_ok());
        assert!(parsed[1].is_err());
        assert!(parsed[2].is_ok());
    }

    #[test]
    fn unknown_event_types_are_ignored() {
        let frame = r#"{"event_type": "tick_size_change", "asset_id": "1", "old_tick_size": "0.01", "new_tick_size": "0.001"}"#;

        assert!(matches!(serde_json::from_str(frame).unwrap(), Event::Other));
    }
}
