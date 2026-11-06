//! Command line entry point: wires the engine and the collector together.

#![warn(clippy::pedantic)]

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
    /// Where to write the SBE stream
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    /// How often to write a full snapshot of every book
    #[arg(long, value_name = "SECONDS", default_value_t = 60)]
    snapshot_interval: u64,
    /// How often to re-list the live assets and adjust subscriptions
    #[arg(long, value_name = "SECONDS", default_value_t = 300)]
    discovery_interval: u64,
    /// Assets per websocket; 100 is the most that still gets every initial book promptly
    #[arg(long, default_value = "100")]
    assets_per_socket: NonZeroUsize,
    /// Subscribe only the first N listed assets (for testing)
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

    let shutdown = CancellationToken::new();
    let writer = AlignedSbeWriter::create(&config.output, engine::CHUNK_SIZE)?;
    let (events, inbox) = mpsc::unbounded_channel();
    let engine = Engine::new(writer, Duration::from_secs(config.snapshot_interval));
    // The engine blocks on 64 MiB writes; on its own thread that never delays a socket.
    let engine = std::thread::Builder::new()
        .name("engine".to_owned())
        .spawn({
            let shutdown = shutdown.clone();
            move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()?
                    .block_on(engine.run(inbox, shutdown))
            }
        })?;

    let mut collector = Collector::new(
        config.assets_per_socket,
        config.limit,
        events,
        shutdown.clone(),
    )?;
    let outcome = tokio::select! {
        result = collector.run(Duration::from_secs(config.discovery_interval)) => result,
        () = shutdown.cancelled() => Ok(()),
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("shutting down");
            Ok(())
        }
    };

    shutdown.cancel();
    collector.close().await;
    engine
        .join()
        .map_err(|_| anyhow::anyhow!("engine thread panicked"))??;
    outcome
}
