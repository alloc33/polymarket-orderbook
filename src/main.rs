use std::{path::PathBuf, time::Duration};

use clap::Parser;
use polymarket_orderbook::{
    connection,
    engine::{self, Engine},
    polymarket::rest,
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
    #[arg(long, default_value_t = 100)]
    limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let config = Config::parse();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let assets = rest::live_assets(&client).await?;
    info!(
        total = assets.len(),
        limit = config.limit,
        "listed live assets"
    );
    let assets: Vec<String> = assets.into_iter().take(config.limit).collect();

    let writer = AlignedSbeWriter::create(&config.output, engine::CHUNK_SIZE)?;
    let engine = Engine::new(writer, Duration::from_secs(config.snapshot_interval));
    let (tx, rx) = mpsc::unbounded_channel();
    let engine = tokio::spawn(engine.run(rx));

    let shutdown = CancellationToken::new();
    let mut connection = tokio::spawn(connection::run(assets, tx, shutdown.clone()));
    let outcome = tokio::select! {
        result = &mut connection => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("shutting down");
            shutdown.cancel();
            connection.await?
        }
    };

    engine.await??;
    outcome
}
