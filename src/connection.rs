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
    exchange::{Exchange, Operation, Socket},
};

// Healthy sockets answer PING within a second, so 30 s without a frame means dead.
// Connects and writes get the same bound so a peer that stops responding cannot
// hold the task forever.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
// A full reconnect now and then clears whatever state the exchange accumulated for
// the socket; spread over ten minutes so the group's sockets never do it together.
const RECONNECT_EVERY: Duration = Duration::from_secs(60 * 60);
const RECONNECT_SPREAD: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
pub enum Command {
    Subscribe(String),
    Unsubscribe(String),
    Resubscribe(String),
}

pub async fn run<E: Exchange>(
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
        let error = match session::<E>(id, &mut subscribed, &mut commands, &engine, &shutdown).await
        {
            Ok(socket) => return close(socket).await,
            Err(error) => error,
        };
        // Tells the engine right away so a redundant socket can take over. A missing
        // engine means the process is already shutting down.
        let _ = engine.send(Inbound::Down(id));
        delay = next_delay(delay, started.elapsed());
        warn!(socket = id, %error, ?delay, "reconnecting");
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay + jitter()) => {}
        }
    }
}

async fn session<E: Exchange>(
    id: usize,
    subscribed: &mut BTreeSet<String>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    engine: &mpsc::UnboundedSender<Inbound>,
    shutdown: &CancellationToken,
) -> anyhow::Result<Socket> {
    let mut socket = tokio::time::timeout(STALL_TIMEOUT, E::connect())
        .await
        .context("connect stalled")??;
    send(&mut socket, E::subscribe(subscribed)).await?;
    debug!(socket = id, assets = subscribed.len(), "subscribed");

    let mut heartbeat = tokio::time::interval(E::HEARTBEAT_INTERVAL);
    let mut last_frame = Instant::now();
    let scheduled = tokio::time::sleep(RECONNECT_EVERY + random(RECONNECT_SPREAD));
    tokio::pin!(scheduled);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(socket),
            () = &mut scheduled => bail!("scheduled reconnect"),
            _ = heartbeat.tick() => send(&mut socket, E::heartbeat()).await?,
            () = tokio::time::sleep_until(last_frame + STALL_TIMEOUT) => {
                bail!("no frames for {STALL_TIMEOUT:?}")
            }
            Some(command) = commands.recv() => {
                apply::<E>(command, &mut socket, subscribed).await?;
            }
            frame = socket.next() => {
                let received_at = Utc::now();
                last_frame = Instant::now();
                let Some(frame) = frame else { bail!("socket closed") };
                let frame = frame?;
                if E::is_heartbeat_reply(&frame) {
                    forward(engine, Inbound::Alive(id))?;
                    continue;
                }
                match frame {
                    Message::Text(text) => dispatch::<E>(&text, id, received_at, engine)?,
                    Message::Close(frame) => bail!("server closed the socket: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

// The set changes before the frame is sent: if the send fails, the reconnect
// resubscribes from the set and the command is not lost.
async fn apply<E: Exchange>(
    command: Command,
    socket: &mut Socket,
    subscribed: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    match command {
        Command::Subscribe(asset) => {
            let frame = E::update(Operation::Subscribe, &asset);
            subscribed.insert(asset);
            send(socket, frame).await
        }
        Command::Unsubscribe(asset) => {
            let frame = E::update(Operation::Unsubscribe, &asset);
            subscribed.remove(&asset);
            send(socket, frame).await
        }
        Command::Resubscribe(asset) => {
            send(socket, E::update(Operation::Unsubscribe, &asset)).await?;
            send(socket, E::update(Operation::Subscribe, &asset)).await
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
    random(Duration::from_secs(1))
}

fn random(up_to: Duration) -> Duration {
    up_to.mul_f64(rand::random_range(0.0..1.0))
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

fn dispatch<E: Exchange>(
    frame: &str,
    id: usize,
    received_at: DateTime<Utc>,
    engine: &mpsc::UnboundedSender<Inbound>,
) -> anyhow::Result<()> {
    for decoded in E::decode(frame) {
        let event = match decoded {
            Ok(event) => event,
            Err(error) => {
                warn!(%error, asset = ?error.asset, "unreadable event");
                if let Some(asset) = error.asset {
                    forward(engine, Inbound::Broken { asset, socket: id })?;
                }
                continue;
            }
        };
        forward(
            engine,
            Inbound::Event {
                socket: id,
                received_at,
                event,
            },
        )?;
    }
    Ok(())
}

fn forward(engine: &mpsc::UnboundedSender<Inbound>, inbound: Inbound) -> anyhow::Result<()> {
    if engine.send(inbound).is_err() {
        bail!("engine stopped");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::Polymarket;

    #[test]
    fn forwards_readable_events_and_reports_broken_ones() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let frame = r#"[
            {"event_type": "book", "asset_id": "1", "timestamp": "1", "bids": [], "asks": []},
            {"event_type": "book", "asset_id": "2", "timestamp": "x", "bids": [], "asks": []},
            {"event_type": "book", "asset_id": "3", "timestamp": "1", "bids": [], "asks": []}
        ]"#;

        dispatch::<Polymarket>(frame, 7, Utc::now(), &tx).unwrap();

        let Inbound::Event { socket: 7, .. } = rx.try_recv().unwrap() else {
            panic!("expected the first book");
        };
        let Inbound::Broken { asset, socket: 7 } = rx.try_recv().unwrap() else {
            panic!("expected a repair request for the bad book");
        };
        assert_eq!(asset, "2");
        let Inbound::Event { socket: 7, .. } = rx.try_recv().unwrap() else {
            panic!("expected the last book");
        };
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
