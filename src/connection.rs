use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::polymarket::ws::{self, Event};

pub async fn run(assets: Vec<String>) -> anyhow::Result<()> {
    let mut socket = ws::connect().await?;
    socket.send(ws::subscribe(&assets)).await?;
    info!(assets = assets.len(), "subscribed");

    let mut ping = tokio::time::interval(ws::PING_INTERVAL);

    loop {
        tokio::select! {
            _ = ping.tick() => socket.send(Message::text(ws::PING)).await?,
            frame = socket.next() => {
                let Some(frame) = frame else { bail!("socket closed") };
                match frame? {
                    Message::Text(text) if text.as_str() == ws::PONG => {}
                    Message::Text(text) => match ws::parse(&text) {
                        Ok(events) => {
                            for event in &events {
                                log(event);
                            }
                        }
                        Err(error) => warn!(%error, "unreadable frame"),
                    },
                    Message::Close(frame) => bail!("server closed the socket: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

fn log(event: &Event) {
    match event {
        Event::Book(book) => info!(
            asset = %book.asset_id,
            bids = book.bids.len(),
            asks = book.asks.len(),
            "book"
        ),
        Event::PriceChange(change) => info!(levels = change.price_changes.len(), "price change"),
        Event::Other => {}
    }
}
