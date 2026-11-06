use std::time::Duration;

use clap::Parser;
use polymarket_orderbook::{connection, polymarket::rest};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Config {
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
    let shutdown = CancellationToken::new();
    let mut connection = tokio::spawn(connection::run(assets, shutdown.clone()));

    tokio::select! {
        result = &mut connection => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("shutting down");
            shutdown.cancel();
            connection.await?
        }
    }
}
