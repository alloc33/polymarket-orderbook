use std::{collections::HashSet, num::NonZeroUsize, time::Duration};

use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    balancer::{Balancer, Placement},
    connection::{self, Command},
    engine::Inbound,
    polymarket::rest,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Collector {
    client: reqwest::Client,
    limit: Option<usize>,
    balancer: Balancer,
    sockets: Vec<mpsc::UnboundedSender<Command>>,
    connections: JoinSet<anyhow::Result<()>>,
    engine: mpsc::UnboundedSender<Inbound>,
    shutdown: CancellationToken,
}

impl Collector {
    pub fn new(
        per_socket: NonZeroUsize,
        limit: Option<usize>,
        engine: mpsc::UnboundedSender<Inbound>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            limit,
            balancer: Balancer::new(per_socket),
            sockets: Vec::new(),
            connections: JoinSet::new(),
            engine,
            shutdown,
        })
    }

    pub async fn run(&mut self, discovery_interval: Duration) -> anyhow::Result<()> {
        self.refresh().await?;

        let mut discovery =
            tokio::time::interval_at(Instant::now() + discovery_interval, discovery_interval);
        discovery.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = discovery.tick() => self.refresh().await?,
                Some(result) = self.connections.join_next() => {
                    if let Err(error) = result? {
                        error!(%error, "connection failed");
                    }
                }
            }
        }
    }

    pub async fn close(mut self) {
        let drain = async {
            while let Some(result) = self.connections.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => error!(%error, "connection failed"),
                    Err(error) => error!(%error, "connection task panicked"),
                }
            }
        };
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, drain).await.is_err() {
            warn!(
                stuck = self.connections.len(),
                "aborting connections that did not close in time"
            );
            self.connections.shutdown().await;
        }
    }

    async fn refresh(&mut self) -> anyhow::Result<()> {
        let (listed, mut listing) = mpsc::unbounded_channel();
        let client = self.client.clone();
        let list = async move { rest::live_assets(&client, &listed).await };

        let mut current = HashSet::new();
        let mut added = 0;
        let place = async {
            while let Some(asset) = listing.recv().await {
                if !current.insert(asset.clone()) {
                    continue;
                }
                if let Some(placement) = self.balancer.add(asset) {
                    added += placement.count();
                    self.place(placement);
                }
                if self.limit.is_some_and(|limit| current.len() >= limit) {
                    break;
                }
            }
            listing.close();
            if let Some(placement) = self.balancer.flush() {
                added += placement.count();
                self.place(placement);
            }
            anyhow::Ok(())
        };
        tokio::try_join!(list, place)?;

        let gone = self.balancer.gone(&current);
        for asset in &gone {
            if let Some(socket) = self.balancer.remove(asset) {
                self.command(socket, Command::Unsubscribe(asset.clone()));
            }
        }

        info!(
            assets = self.balancer.count(),
            sockets = self.sockets.len(),
            added,
            removed = gone.len(),
            "refreshed live assets"
        );
        Ok(())
    }

    fn place(&mut self, placement: Placement) {
        match placement {
            Placement::Existing { socket, asset } => {
                self.command(socket, Command::Subscribe(asset));
            }
            Placement::New { assets, .. } => {
                let (commands, inbox) = mpsc::unbounded_channel();
                self.connections.spawn(connection::run(
                    assets,
                    inbox,
                    self.engine.clone(),
                    self.shutdown.clone(),
                ));
                self.sockets.push(commands);
            }
        }
    }

    fn command(&self, socket: usize, command: Command) {
        if self.sockets[socket].send(command).is_err() {
            warn!(socket, "connection is gone, command dropped");
        }
    }
}
