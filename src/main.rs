use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use clap::Parser;
use polymarket_orderbook::{
    balancer::Balancer,
    connection,
    engine::{self, Engine, Inbound},
    polymarket::rest,
    util::AlignedSbeWriter,
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Parser)]
struct Config {
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    #[arg(long, value_name = "SECONDS", default_value_t = 60)]
    snapshot_interval: u64,
    #[arg(long, default_value = "100")]
    assets_per_socket: NonZeroUsize,
    #[arg(long)]
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let config = Config::parse();

    let writer = AlignedSbeWriter::create(&config.output, engine::CHUNK_SIZE)?;
    let (events, inbox) = mpsc::unbounded_channel();
    let engine = Engine::new(writer, Duration::from_secs(config.snapshot_interval));
    let engine = tokio::spawn(engine.run(inbox));

    let shutdown = CancellationToken::new();
    let mut connections = JoinSet::new();
    let outcome = tokio::select! {
        result = collect(&config, &events, &shutdown, &mut connections) => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("shutting down");
            Ok(())
        }
    };

    shutdown.cancel();
    drop(events);
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, drain(&mut connections))
        .await
        .is_err()
    {
        warn!(
            stuck = connections.len(),
            "aborting connections that did not close in time"
        );
        connections.shutdown().await;
    }
    engine.await??;
    outcome
}

async fn collect(
    config: &Config,
    events: &mpsc::UnboundedSender<Inbound>,
    shutdown: &CancellationToken,
    connections: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let (assets, mut listed) = mpsc::unbounded_channel();
    let listing = async move { rest::live_assets(&client, &assets).await };

    let mut balancer = Balancer::new(config.assets_per_socket);
    let subscribing = async {
        let mut spawn = |batch| {
            connections.spawn(connection::run(batch, events.clone(), shutdown.clone()));
        };
        while let Some(asset) = listed.recv().await {
            if let Some(batch) = balancer.add(asset) {
                spawn(batch);
            }
            if config.limit.is_some_and(|limit| balancer.count() >= limit) {
                break;
            }
        }
        if let Some(batch) = balancer.flush() {
            spawn(batch);
        }
        anyhow::Ok(())
    };
    tokio::try_join!(listing, subscribing)?;
    info!(
        assets = balancer.count(),
        sockets = connections.len(),
        "subscribed all live assets"
    );

    drain(connections).await
}

async fn drain(connections: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result? {
            error!(%error, "connection failed");
        }
    }
    Ok(())
}
