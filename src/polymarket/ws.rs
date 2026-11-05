use std::time::Duration;

use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

const URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const PING: &str = "PING";
pub const PONG: &str = "PONG";
pub const PING_INTERVAL: Duration = Duration::from_secs(10);

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect() -> anyhow::Result<Socket> {
    let (socket, _) = connect_async(URL).await?;
    Ok(socket)
}

pub fn subscribe(assets_ids: &[String]) -> Message {
    Message::text(serde_json::json!({ "assets_ids": assets_ids, "type": "market" }).to_string())
}

pub fn parse(frame: &str) -> serde_json::Result<Vec<Event>> {
    if frame.starts_with('[') {
        serde_json::from_str(frame)
    } else {
        serde_json::from_str(frame).map(|event| vec![event])
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

    #[test]
    fn subscribe_message_has_documented_shape() {
        let assets = ["123".to_owned(), "456".to_owned()];

        let message = subscribe(&assets).into_text().unwrap();
        let json: serde_json::Value = serde_json::from_str(message.as_str()).unwrap();

        assert_eq!(
            json,
            serde_json::json!({ "assets_ids": ["123", "456"], "type": "market" })
        );
    }

    #[test]
    fn book_event_parses() {
        let frame = r#"{
            "event_type": "book",
            "asset_id": "32338220190071351435",
            "market": "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7",
            "timestamp": "1793811735726",
            "hash": "0xabc123",
            "bids": [{"price": "0.017", "size": "800"}, {"price": "0.018", "size": "1254.57"}],
            "asks": [{"price": "0.019", "size": "5884.04"}]
        }"#;

        let Event::Book(book) = serde_json::from_str(frame).unwrap() else {
            panic!("expected a book event");
        };

        assert_eq!(book.asset_id, "32338220190071351435");
        assert_eq!(book.timestamp, "1793811735726");
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.bids[1].price, "0.018");
        assert_eq!(book.bids[1].size, "1254.57");
        assert_eq!(book.asks.len(), 1);
    }

    #[test]
    fn price_change_event_parses() {
        let frame = r#"{
            "event_type": "price_change",
            "market": "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7",
            "timestamp": "1793811751973",
            "price_changes": [
                {"asset_id": "25659310674993675562", "price": "0.29", "size": "8275", "side": "BUY",
                 "hash": "57d914ca", "best_bid": "0.981", "best_ask": "0.982"},
                {"asset_id": "32338220190071351435", "price": "0.71", "size": "0", "side": "SELL",
                 "hash": "57d914ca", "best_bid": "0.018", "best_ask": "0.019"}
            ]
        }"#;

        let Event::PriceChange(change) = serde_json::from_str(frame).unwrap() else {
            panic!("expected a price change event");
        };

        assert_eq!(change.timestamp, "1793811751973");
        assert_eq!(change.price_changes.len(), 2);
        assert_eq!(change.price_changes[0].side, OrderSide::Buy);
        assert_eq!(change.price_changes[0].size, "8275");
        assert_eq!(change.price_changes[1].side, OrderSide::Sell);
        assert_eq!(change.price_changes[1].size, "0");
    }

    #[test]
    fn frame_may_hold_one_event_or_a_batch() {
        let one =
            r#"{"event_type": "book", "asset_id": "1", "timestamp": "1", "bids": [], "asks": []}"#;
        let batch = r#"[
            {"event_type": "book", "asset_id": "1", "timestamp": "1", "bids": [], "asks": []},
            {"event_type": "book", "asset_id": "2", "timestamp": "1", "bids": [], "asks": []}
        ]"#;

        assert_eq!(parse(one).unwrap().len(), 1);
        assert_eq!(parse(batch).unwrap().len(), 2);
    }

    #[test]
    fn unknown_event_types_are_ignored() {
        let frame = r#"{"event_type": "tick_size_change", "asset_id": "1", "old_tick_size": "0.01", "new_tick_size": "0.001"}"#;

        assert!(matches!(serde_json::from_str(frame).unwrap(), Event::Other));
    }
}
