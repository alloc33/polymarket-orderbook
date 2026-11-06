use std::path::PathBuf;

use polymarket_orderbook::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};

fn main() -> anyhow::Result<()> {
    let path: PathBuf = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: dump <file.sbe>"))?
        .into();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

    let mut snapshots = 0usize;
    let mut updates = 0usize;
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                snapshots += 1;
                if snapshots <= 3 {
                    println!(
                        "snapshot {} received_at={} exchange={} bids={} asks={}",
                        snapshot.symbol,
                        snapshot.received_at,
                        snapshot.exchange_timestamp,
                        snapshot.bids.len(),
                        snapshot.asks.len()
                    );
                }
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                updates += 1;
                if updates <= 3 {
                    println!(
                        "update {} received_at={} exchange={} {} {} -> {}",
                        update.symbol,
                        update.backtest_timestamp,
                        update.exchange_timestamp,
                        update.side,
                        update.price_level,
                        update.quantity
                    );
                }
            }
            _ => {}
        }
    }

    println!("{snapshots} snapshots, {updates} price level updates");
    Ok(())
}
