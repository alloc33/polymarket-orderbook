use std::collections::{HashMap, HashSet};

use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::{
    book::{self, OrderBook},
    polymarket::ws::{self, Event},
};

pub async fn run(assets: Vec<String>) -> anyhow::Result<()> {
    let mut socket = ws::connect().await?;
    socket.send(ws::subscribe(&assets)).await?;
    info!(assets = assets.len(), "subscribed");

    let subscribed: HashSet<String> = assets.into_iter().collect();
    let mut books = HashMap::new();
    let mut ping = tokio::time::interval(ws::PING_INTERVAL);

    loop {
        tokio::select! {
            _ = ping.tick() => socket.send(Message::text(ws::PING)).await?,
            frame = socket.next() => {
                let Some(frame) = frame else { bail!("socket closed") };
                match frame? {
                    Message::Text(text) if text.as_str() == ws::PONG => {}
                    Message::Text(text) => dispatch(&text, &subscribed, &mut books),
                    Message::Close(frame) => bail!("server closed the socket: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

fn dispatch(frame: &str, subscribed: &HashSet<String>, books: &mut HashMap<String, OrderBook>) {
    let events = match ws::parse(frame) {
        Ok(events) => events,
        Err(error) => {
            warn!(%error, "unreadable frame");
            return;
        }
    };
    for event in events {
        if let Err(error) = handle(subscribed, books, event) {
            warn!(%error, "ignored event");
        }
    }
}

fn handle(
    subscribed: &HashSet<String>,
    books: &mut HashMap<String, OrderBook>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Book(book) => {
            let order_book = OrderBook::try_from(&book)?;
            info!(
                asset = %book.asset_id,
                best_bid = ?order_book.best_bid(),
                best_ask = ?order_book.best_ask(),
                "book"
            );
            books.insert(book.asset_id, order_book);
        }
        Event::PriceChange(change) => {
            let exchange_ts = book::parse_timestamp(&change.timestamp)?;
            for level in &change.price_changes {
                if !subscribed.contains(&level.asset_id) {
                    continue;
                }
                let Some(order_book) = books.get_mut(&level.asset_id) else {
                    warn!(asset = %level.asset_id, "price change before book");
                    continue;
                };
                let update = order_book.apply(level, exchange_ts)?;
                info!(
                    asset = %level.asset_id,
                    side = %update.side,
                    price = update.price,
                    quantity = update.quantity,
                    best_bid = ?order_book.best_bid(),
                    best_ask = ?order_book.best_ask(),
                    "level"
                );
            }
        }
        Event::Other => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::ws::{LevelChange, OrderSide, PriceChange};

    #[test]
    fn changes_for_other_assets_are_ignored() {
        let subscribed = HashSet::from(["1".to_owned()]);
        let mut books = HashMap::new();
        let event = Event::PriceChange(PriceChange {
            timestamp: "1793811751973".to_owned(),
            price_changes: vec![LevelChange {
                asset_id: "2".to_owned(),
                price: "0.5".to_owned(),
                size: "10".to_owned(),
                side: OrderSide::Buy,
            }],
        });

        handle(&subscribed, &mut books, event).unwrap();

        assert!(books.is_empty());
    }
}
