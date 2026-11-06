use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use clap::Parser;
use polymarket_orderbook::{
    collector::Collector,
    engine::{self, Engine},
    util::AlignedSbeWriter,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Config {
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    #[arg(long, value_name = "SECONDS", default_value_t = 60)]
    snapshot_interval: u64,
    #[arg(long, value_name = "SECONDS", default_value_t = 300)]
    discovery_interval: u64,
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
    let mut collector = Collector::new(
        config.assets_per_socket,
        config.limit,
        events,
        shutdown.clone(),
    )?;
    let outcome = tokio::select! {
        result = collector.run(Duration::from_secs(config.discovery_interval)) => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("shutting down");
            Ok(())
        }
    };

    shutdown.cancel();
    collector.close().await;
    engine.await??;
    outcome
}
