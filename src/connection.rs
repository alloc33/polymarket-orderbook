use std::{collections::HashSet, time::Duration};

use anyhow::bail;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    engine::Inbound,
    polymarket::ws::{self, Event, Socket},
};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(
    assets: Vec<String>,
    engine: mpsc::UnboundedSender<Inbound>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut socket = ws::connect().await?;
    socket.send(ws::subscribe(&assets)).await?;
    debug!(assets = assets.len(), "subscribed");

    let subscribed: HashSet<String> = assets.into_iter().collect();
    let mut ping = tokio::time::interval(ws::PING_INTERVAL);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return close(socket).await,
            _ = ping.tick() => socket.send(Message::text(ws::PING)).await?,
            frame = socket.next() => {
                let received_at = Utc::now();
                let Some(frame) = frame else { bail!("socket closed") };
                match frame? {
                    Message::Text(text) if text.as_str() == ws::PONG => {}
                    Message::Text(text) => dispatch(&text, received_at, &subscribed, &engine)?,
                    Message::Close(frame) => bail!("server closed the socket: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

async fn close(mut socket: Socket) -> anyhow::Result<()> {
    let handshake = async {
        socket.close(None).await?;
        while let Some(frame) = socket.next().await {
            if matches!(frame, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
        anyhow::Ok(())
    };
    match tokio::time::timeout(CLOSE_TIMEOUT, handshake).await {
        Ok(result) => result?,
        Err(_) => warn!("close handshake timed out"),
    }
    debug!("socket closed");
    Ok(())
}

fn dispatch(
    frame: &str,
    received_at: DateTime<Utc>,
    subscribed: &HashSet<String>,
    engine: &mpsc::UnboundedSender<Inbound>,
) -> anyhow::Result<()> {
    let events = match ws::parse(frame) {
        Ok(events) => events,
        Err(error) => {
            warn!(%error, "unreadable frame");
            return Ok(());
        }
    };
    for mut event in events {
        if let Event::PriceChange(change) = &mut event {
            change
                .price_changes
                .retain(|level| subscribed.contains(&level.asset_id));
            if change.price_changes.is_empty() {
                continue;
            }
        }
        if engine.send(Inbound { received_at, event }).is_err() {
            bail!("engine stopped");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::ws::{LevelChange, OrderSide};

    fn change(asset_id: &str) -> LevelChange {
        LevelChange {
            asset_id: asset_id.to_owned(),
            price: "0.5".to_owned(),
            size: "10".to_owned(),
            side: OrderSide::Buy,
        }
    }

    #[test]
    fn forwards_only_subscribed_assets() {
        let subscribed = HashSet::from(["1".to_owned()]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let frame = serde_json::json!({
            "event_type": "price_change",
            "timestamp": "1793811751973",
            "price_changes": [
                {"asset_id": "1", "price": "0.5", "size": "10", "side": "BUY"},
                {"asset_id": "2", "price": "0.5", "size": "10", "side": "BUY"}
            ]
        })
        .to_string();

        dispatch(&frame, Utc::now(), &subscribed, &tx).unwrap();

        let Event::PriceChange(forwarded) = rx.try_recv().unwrap().event else {
            panic!("expected a price change");
        };
        assert_eq!(forwarded.price_changes.len(), 1);
        assert_eq!(forwarded.price_changes[0].asset_id, change("1").asset_id);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn drops_price_changes_with_no_subscribed_assets() {
        let subscribed = HashSet::from(["1".to_owned()]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let frame = serde_json::json!({
            "event_type": "price_change",
            "timestamp": "1793811751973",
            "price_changes": [{"asset_id": "2", "price": "0.5", "size": "10", "side": "BUY"}]
        })
        .to_string();

        dispatch(&frame, Utc::now(), &subscribed, &tx).unwrap();

        assert!(rx.try_recv().is_err());
    }
}
