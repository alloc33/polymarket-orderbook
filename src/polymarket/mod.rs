//! Polymarket: the Gamma REST listing and the CLOB market websocket.

use std::{collections::BTreeSet, time::Duration};

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    event::Event,
    exchange::{Exchange, Operation, Socket},
};

mod rest;
mod ws;

pub struct Polymarket;

impl Exchange for Polymarket {
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

    async fn list_assets(
        client: &reqwest::Client,
        assets: &mpsc::UnboundedSender<String>,
    ) -> anyhow::Result<()> {
        rest::live_assets(client, assets).await
    }

    async fn connect() -> anyhow::Result<Socket> {
        ws::connect().await
    }

    fn subscribe(assets: &BTreeSet<String>) -> Message {
        ws::subscribe(assets)
    }

    fn update(operation: Operation, asset: &str) -> Message {
        ws::update(operation, asset)
    }

    fn heartbeat() -> Message {
        Message::text(ws::PING)
    }

    fn is_heartbeat_reply(message: &Message) -> bool {
        matches!(message, Message::Text(text) if text.as_str() == ws::PONG)
    }

    fn decode(frame: &str) -> Vec<anyhow::Result<Event>> {
        ws::decode(frame)
    }
}
