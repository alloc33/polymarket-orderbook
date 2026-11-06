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
    polymarket::ws::{self, Operation, Socket},
};

// Healthy sockets answer PING within a second, so 30 s without a frame means dead.
// Connects and writes get the same bound so a peer that stops responding cannot
// hold the task forever.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub enum Command {
    Subscribe(String),
    Unsubscribe(String),
}

pub async fn run(
    id: usize,
    assets: Vec<String>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    engine: mpsc::UnboundedSender<Inbound>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut subscribed: BTreeSet<String> = assets.into_iter().collect();
    let mut delay = RECONNECT_DELAY;

    loop {
        let started = Instant::now();
        let error = match session(id, &mut subscribed, &mut commands, &engine, &shutdown).await {
            Ok(socket) => return close(socket).await,
            Err(error) => error,
        };
        delay = next_delay(delay, started.elapsed());
        warn!(socket = id, %error, ?delay, "reconnecting");
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay + jitter()) => {}
        }
    }
}

async fn session(
    id: usize,
    subscribed: &mut BTreeSet<String>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    engine: &mpsc::UnboundedSender<Inbound>,
    shutdown: &CancellationToken,
) -> anyhow::Result<Socket> {
    let mut socket = tokio::time::timeout(STALL_TIMEOUT, ws::connect())
        .await
        .context("connect stalled")??;
    send(&mut socket, ws::subscribe(subscribed)).await?;
    debug!(socket = id, assets = subscribed.len(), "subscribed");

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
                apply(command, &mut socket, subscribed).await?;
            }
            frame = socket.next() => {
                let received_at = Utc::now();
                last_frame = Instant::now();
                let Some(frame) = frame else { bail!("socket closed") };
                match frame? {
                    Message::Text(text) if text.as_str() == ws::PONG => {}
                    Message::Text(text) => dispatch(&text, id, received_at, engine)?,
                    Message::Close(frame) => bail!("server closed the socket: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

// The set changes before the frame is sent: if the send fails, the reconnect
// resubscribes from the set and the command is not lost.
async fn apply(
    command: Command,
    socket: &mut Socket,
    subscribed: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    match command {
        Command::Subscribe(asset) => {
            let frame = ws::update(Operation::Subscribe, &asset);
            subscribed.insert(asset);
            send(socket, frame).await
        }
        Command::Unsubscribe(asset) => {
            let frame = ws::update(Operation::Unsubscribe, &asset);
            subscribed.remove(&asset);
            send(socket, frame).await
        }
    }
}

fn next_delay(delay: Duration, session_lasted: Duration) -> Duration {
    if session_lasted > MAX_RECONNECT_DELAY {
        RECONNECT_DELAY
    } else {
        (delay * 2).min(MAX_RECONNECT_DELAY)
    }
}

// Spreads reconnects out so that an outage does not end with every socket
// connecting in the same second.
fn jitter() -> Duration {
    Duration::from_millis(rand::random_range(0..1000))
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
    id: usize,
    received_at: DateTime<Utc>,
    engine: &mpsc::UnboundedSender<Inbound>,
) -> anyhow::Result<()> {
    for event in ws::parse(frame) {
        let Ok(event) = event.inspect_err(|error| warn!(%error, "unreadable event")) else {
            continue;
        };
        let inbound = Inbound::Event {
            socket: id,
            received_at,
            event,
        };
        if engine.send(inbound).is_err() {
            bail!("engine stopped");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_every_readable_event_tagged_with_the_socket() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let frame = r#"[
            {"event_type": "book", "asset_id": "1", "timestamp": "1", "bids": [], "asks": []},
            {"event_type": "book", "asset_id": 2},
            {"event_type": "book", "asset_id": "3", "timestamp": "1", "bids": [], "asks": []}
        ]"#;

        dispatch(frame, 7, Utc::now(), &tx).unwrap();

        for _ in 0..2 {
            let Inbound::Event { socket, .. } = rx.try_recv().unwrap() else {
                panic!("expected an event");
            };
            assert_eq!(socket, 7);
        }
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
