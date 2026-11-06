//! The exchange-specific surface: listing live assets and speaking the websocket
//! protocol. Everything else in the crate works on `event::Event`.

use std::{collections::BTreeSet, future::Future, time::Duration};

use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::event::Event;

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Subscribe,
    Unsubscribe,
}

pub trait Exchange: Send + Sync + 'static {
    const HEARTBEAT_INTERVAL: Duration;

    fn list_assets(
        client: &reqwest::Client,
        assets: &mpsc::UnboundedSender<String>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn connect() -> impl Future<Output = anyhow::Result<Socket>> + Send;

    fn subscribe(assets: &BTreeSet<String>) -> Message;

    fn update(operation: Operation, asset: &str) -> Message;

    fn heartbeat() -> Message;

    fn is_heartbeat_reply(message: &Message) -> bool;

    fn decode(frame: &str) -> Vec<anyhow::Result<Event>>;
}
