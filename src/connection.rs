//! One websocket: subscribes its assets, forwards events to the engine and
//! reconnects for as long as the process runs.

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    engine::Inbound,
    polymarket::ws::{self, Event, Operation, Socket},
};

// Healthy sockets answer PING within a second, so 30 s without a frame means dead.
// Writes get the same bound: a wedged send once blocked shutdown for good.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub enum Command {
    Subscribe(String),
    Unsubscribe(String),
}

pub async fn run(
    assets: Vec<String>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    engine: mpsc::UnboundedSender<Inbound>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut subscribed: BTreeSet<String> = assets.into_iter().collect();
    let mut delay = RECONNECT_DELAY;

    loop {
        let started = Instant::now();
        let error = match session(&mut subscribed, &mut commands, &engine, &shutdown).await {
            Ok(socket) => return close(socket).await,
            Err(error) => error,
        };
        delay = next_delay(delay, started.elapsed());
        warn!(%error, ?delay, "reconnecting");
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn session(
    subscribed: &mut BTreeSet<String>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    engine: &mpsc::UnboundedSender<Inbound>,
    shutdown: &CancellationToken,
) -> anyhow::Result<Socket> {
    let mut socket = ws::connect().await?;
    send(&mut socket, ws::subscribe(subscribed)).await?;
    debug!(assets = subscribed.len(), "subscribed");

    let mut ping = tokio::time::interval(ws::PING_INTERVAL);
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(socket),
            _ = ping.tick() => send(&mut socket, Message::text(ws::PING)).await?,
            () = tokio::time::sleep_until(last_frame + STALL_TIMEOUT) => {
                bail!("no frames for {STALL_TIMEOUT:?}")
            }
            Some(command) = commands.recv() => {
                apply(command, &mut socket, subscribed, engine).await?;
            }
            frame = socket.next() => {
                let received_at = Utc::now();
                last_frame = Instant::now();
                let Some(frame) = frame else { bail!("socket closed") };
                match frame? {
                    Message::Text(text) if text.as_str() == ws::PONG => {}
                    Message::Text(text) => dispatch(&text, received_at, subscribed, engine)?,
                    Message::Close(frame) => bail!("server closed the socket: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

async fn apply(
    command: Command,
    socket: &mut Socket,
    subscribed: &mut BTreeSet<String>,
    engine: &mpsc::UnboundedSender<Inbound>,
) -> anyhow::Result<()> {
    match command {
        Command::Subscribe(asset) => {
            send(socket, ws::update(Operation::Subscribe, &asset)).await?;
            subscribed.insert(asset);
        }
        Command::Unsubscribe(asset) => {
            send(socket, ws::update(Operation::Unsubscribe, &asset)).await?;
            subscribed.remove(&asset);
            // Sent from here so it cannot overtake an update this socket already forwarded.
            if engine.send(Inbound::Forget(asset)).is_err() {
                bail!("engine stopped");
            }
        }
    }
    Ok(())
}

fn next_delay(delay: Duration, session_lasted: Duration) -> Duration {
    if session_lasted > MAX_RECONNECT_DELAY {
        RECONNECT_DELAY
    } else {
        (delay * 2).min(MAX_RECONNECT_DELAY)
    }
}

async fn send(socket: &mut Socket, message: Message) -> anyhow::Result<()> {
    tokio::time::timeout(STALL_TIMEOUT, socket.send(message))
        .await
        .context("send stalled")??;
    Ok(())
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
    if let Ok(result) = tokio::time::timeout(CLOSE_TIMEOUT, handshake).await {
        result?;
    } else {
        warn!("close handshake timed out");
    }
    debug!("socket closed");
    Ok(())
}

fn dispatch(
    frame: &str,
    received_at: DateTime<Utc>,
    subscribed: &BTreeSet<String>,
    engine: &mpsc::UnboundedSender<Inbound>,
) -> anyhow::Result<()> {
    let Ok(events) = ws::parse(frame).inspect_err(|error| warn!(%error, "unreadable frame")) else {
        return Ok(());
    };
    // A price change is per market, so it also carries the sibling token's lines.
    // Keeping only this socket's assets avoids duplicates when the sibling lives
    // on another socket.
    for mut event in events {
        match &mut event {
            Event::Book(book) if !subscribed.contains(&book.asset_id) => continue,
            Event::PriceChange(change) => {
                change
                    .price_changes
                    .retain(|level| subscribed.contains(&level.asset_id));
                if change.price_changes.is_empty() {
                    continue;
                }
            }
            _ => {}
        }
        if engine.send(Inbound::Event { received_at, event }).is_err() {
            bail!("engine stopped");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price_change(assets: &[&str]) -> String {
        let changes: Vec<_> = assets
            .iter()
            .map(|asset| {
                serde_json::json!({"asset_id": asset, "price": "0.5", "size": "10", "side": "BUY"})
            })
            .collect();
        serde_json::json!({
            "event_type": "price_change",
            "timestamp": "1793811751973",
            "price_changes": changes
        })
        .to_string()
    }

    #[test]
    fn forwards_only_subscribed_assets() {
        let subscribed = BTreeSet::from(["1".to_owned()]);
        let (tx, mut rx) = mpsc::unbounded_channel();

        dispatch(&price_change(&["1", "2"]), Utc::now(), &subscribed, &tx).unwrap();

        let Inbound::Event {
            event: Event::PriceChange(forwarded),
            ..
        } = rx.try_recv().unwrap()
        else {
            panic!("expected a price change");
        };
        assert_eq!(forwarded.price_changes.len(), 1);
        assert_eq!(forwarded.price_changes[0].asset_id, "1");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn drops_events_with_no_subscribed_assets() {
        let subscribed = BTreeSet::from(["1".to_owned()]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let book = serde_json::json!({
            "event_type": "book", "asset_id": "2", "timestamp": "1", "bids": [], "asks": []
        })
        .to_string();

        dispatch(&price_change(&["2"]), Utc::now(), &subscribed, &tx).unwrap();
        dispatch(&book, Utc::now(), &subscribed, &tx).unwrap();

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reconnect_delay_doubles_caps_and_resets_after_a_stable_session() {
        let short = Duration::from_secs(1);

        assert_eq!(
            next_delay(Duration::from_secs(1), short),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_delay(Duration::from_secs(16), short),
            Duration::from_secs(30)
        );
        assert_eq!(
            next_delay(Duration::from_secs(30), short),
            Duration::from_secs(30)
        );
        assert_eq!(
            next_delay(Duration::from_secs(30), Duration::from_secs(600)),
            Duration::from_secs(1)
        );
    }
}
