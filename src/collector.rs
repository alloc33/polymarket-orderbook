//! Keeps the subscribed set equal to the exchange's live assets.

use std::{collections::HashSet, marker::PhantomData, num::NonZeroUsize, time::Duration};

use tokio::{sync::mpsc, task::JoinSet, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    balancer::{Balancer, Placement},
    connection::{self, Command},
    engine::{Inbound, Repair},
    exchange::Exchange,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
// Sockets that cannot close politely in time are aborted.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Collector<E> {
    exchange: PhantomData<E>,
    client: reqwest::Client,
    sockets_per_group: NonZeroUsize,
    limit: Option<usize>,
    balancer: Balancer,
    sockets: Vec<mpsc::UnboundedSender<Command>>,
    groups: Vec<Vec<usize>>,
    connections: JoinSet<anyhow::Result<()>>,
    engine: mpsc::UnboundedSender<Inbound>,
    repairs: mpsc::UnboundedReceiver<Repair>,
    shutdown: CancellationToken,
}

impl<E: Exchange> Collector<E> {
    pub fn new(
        assets_per_group: NonZeroUsize,
        sockets_per_group: NonZeroUsize,
        limit: Option<usize>,
        engine: mpsc::UnboundedSender<Inbound>,
        repairs: mpsc::UnboundedReceiver<Repair>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            exchange: PhantomData,
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            sockets_per_group,
            limit,
            balancer: Balancer::new(assets_per_group),
            sockets: Vec::new(),
            groups: Vec::new(),
            connections: JoinSet::new(),
            engine,
            repairs,
            shutdown,
        })
    }

    pub async fn run(&mut self, discovery_interval: Duration) -> anyhow::Result<()> {
        let mut discovery = tokio::time::interval(discovery_interval);
        discovery.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // A failed round keeps what it placed so far; the next tick finishes the job.
                _ = discovery.tick() => {
                    if let Err(error) = self.refresh().await {
                        error!(%error, "discovery round failed");
                    }
                }
                Some(repair) = self.repairs.recv() => self.repair(repair),
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
        let list = async move { E::list_assets(&client, &listed).await };

        let mut current = HashSet::new();
        let before = self.balancer.count();
        let place = async {
            while let Some(asset) = listing.recv().await {
                if !current.insert(asset.clone()) {
                    continue;
                }
                if let Some(placement) = self.balancer.add(asset) {
                    self.place(placement);
                }
                if self.limit.is_some_and(|limit| current.len() >= limit) {
                    break;
                }
            }
            listing.close();
            if let Some(placement) = self.balancer.flush() {
                self.place(placement);
            }
            anyhow::Ok(())
        };
        tokio::try_join!(list, place)?;
        let added = self.balancer.count() - before;

        let gone = self.balancer.gone(&current);
        for asset in &gone {
            if let Some(group) = self.balancer.remove(asset) {
                self.notify(Inbound::Forget(asset.clone()));
                self.command(group, &Command::Unsubscribe(asset.clone()));
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

    // The engine learns which sockets carry an asset before any of them can send.
    fn place(&mut self, placement: Placement) {
        match placement {
            Placement::Existing { group, asset } => {
                self.notify(Inbound::Assign {
                    asset: asset.clone(),
                    sockets: self.groups[group].clone(),
                });
                self.command(group, &Command::Subscribe(asset));
            }
            Placement::New(assets) => {
                let first = self.sockets.len();
                let sockets: Vec<usize> = (first..first + self.sockets_per_group.get()).collect();
                for asset in &assets {
                    self.notify(Inbound::Assign {
                        asset: asset.clone(),
                        sockets: sockets.clone(),
                    });
                }
                for &socket in &sockets {
                    let (commands, inbox) = mpsc::unbounded_channel();
                    self.connections.spawn(connection::run::<E>(
                        socket,
                        assets.clone(),
                        inbox,
                        self.engine.clone(),
                        self.shutdown.clone(),
                    ));
                    self.sockets.push(commands);
                }
                self.groups.push(sockets);
            }
        }
    }

    fn repair(&self, repair: Repair) {
        let command = Command::Resubscribe(repair.asset);
        if self.sockets[repair.socket].send(command).is_err() {
            warn!(socket = repair.socket, "connection is gone, repair dropped");
        }
    }

    fn command(&self, group: usize, command: &Command) {
        for &socket in &self.groups[group] {
            if self.sockets[socket].send(command.clone()).is_err() {
                warn!(socket, "connection is gone, command dropped");
            }
        }
    }

    // An engine that stopped has already cancelled the shutdown token.
    fn notify(&self, inbound: Inbound) {
        let _ = self.engine.send(inbound);
    }
}
