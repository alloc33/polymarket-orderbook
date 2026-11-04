use std::time::Duration;

use polymarket_orderbook::polymarket::rest;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let tokens = rest::live_tokens(&client).await?;

    println!("{} live tokens", tokens.len());
    for token in tokens.iter().take(10) {
        println!("{token}");
    }

    Ok(())
}
