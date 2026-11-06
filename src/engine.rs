use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use tracing::{info, warn};

use crate::{
    book::{self, LevelUpdate, OrderBook},
    polymarket::ws::Event,
    util::{
        AlignedSbeWriter,
        dtos::challenge_orderbook::{
            PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
            SnapshotDomain,
        },
    },
};

pub const CHUNK_SIZE: usize = 64 << 20;

pub struct Inbound {
    pub received_at: DateTime<Utc>,
    pub event: Event,
}

pub struct Engine {
    books: HashMap<String, OrderBook>,
    writer: AlignedSbeWriter,
    snapshot_interval: Duration,
}

impl Engine {
    pub fn new(writer: AlignedSbeWriter, snapshot_interval: Duration) -> Self {
        Self {
            books: HashMap::new(),
            writer,
            snapshot_interval,
        }
    }

    pub async fn run(
        mut self,
        mut inbound: mpsc::UnboundedReceiver<Inbound>,
    ) -> anyhow::Result<()> {
        let mut snapshots = tokio::time::interval_at(
            Instant::now() + self.snapshot_interval,
            self.snapshot_interval,
        );
        snapshots.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                received = inbound.recv() => match received {
                    Some(inbound) => self.handle(inbound)?,
                    None => return self.writer.flush(),
                },
                _ = snapshots.tick() => self.snapshot_all()?,
            }
        }
    }

    fn handle(&mut self, inbound: Inbound) -> anyhow::Result<()> {
        match inbound.event {
            Event::Book(book) => {
                let order_book = match OrderBook::try_from(&book) {
                    Ok(order_book) => order_book,
                    Err(error) => {
                        warn!(%error, asset = %book.asset_id, "ignored book");
                        return Ok(());
                    }
                };
                self.writer.write_msg(&snapshot(
                    &book.asset_id,
                    &order_book,
                    inbound.received_at,
                ))?;
                self.books.insert(book.asset_id, order_book);
            }
            Event::PriceChange(change) => {
                let exchange_ts = match book::parse_timestamp(&change.timestamp) {
                    Ok(exchange_ts) => exchange_ts,
                    Err(error) => {
                        warn!(%error, "ignored price change");
                        return Ok(());
                    }
                };
                for level in &change.price_changes {
                    let Some(order_book) = self.books.get_mut(&level.asset_id) else {
                        warn!(asset = %level.asset_id, "price change before book");
                        continue;
                    };
                    let update = match order_book.apply(level, exchange_ts) {
                        Ok(update) => update,
                        Err(error) => {
                            warn!(%error, asset = %level.asset_id, "ignored level");
                            continue;
                        }
                    };
                    self.writer.write_msg(&level_update(
                        &level.asset_id,
                        update,
                        inbound.received_at,
                        exchange_ts,
                    ))?;
                }
            }
            Event::Other => {}
        }
        Ok(())
    }

    fn snapshot_all(&mut self) -> anyhow::Result<()> {
        let received_at = Utc::now();
        for (symbol, book) in &self.books {
            self.writer
                .write_msg(&snapshot(symbol, book, received_at))?;
        }
        info!(books = self.books.len(), "snapshot round");
        Ok(())
    }
}

fn snapshot(symbol: &str, book: &OrderBook, received_at: DateTime<Utc>) -> SnapshotDomain {
    SnapshotDomain {
        received_at,
        exchange_timestamp: book.exchange_ts,
        bids: book
            .bids
            .iter()
            .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: symbol.to_owned(),
    }
}

fn level_update(
    symbol: &str,
    update: LevelUpdate,
    received_at: DateTime<Utc>,
    exchange_ts: DateTime<Utc>,
) -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        backtest_timestamp: received_at,
        exchange_timestamp: exchange_ts,
        price_level: update.price,
        quantity: update.quantity,
        side: update.side,
        symbol: symbol.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Side,
        polymarket::ws::{Book, Level, LevelChange, OrderSide, PriceChange},
        util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage},
    };

    fn level(price: &str, size: &str) -> Level {
        Level {
            price: price.to_owned(),
            size: size.to_owned(),
        }
    }

    #[tokio::test]
    async fn writes_snapshot_then_level_update_and_flushes_on_close() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE).unwrap();
        let engine = Engine::new(writer, Duration::from_secs(3600));
        let (tx, rx) = mpsc::unbounded_channel();
        let received_at = DateTime::from_timestamp_millis(1_793_811_735_800).unwrap();

        tx.send(Inbound {
            received_at,
            event: Event::Book(Book {
                asset_id: "1".to_owned(),
                timestamp: "1793811735726".to_owned(),
                bids: vec![level("0.017", "800"), level("0.018", "1254.57")],
                asks: vec![level("0.019", "5884.04")],
            }),
        })
        .unwrap();
        tx.send(Inbound {
            received_at,
            event: Event::PriceChange(PriceChange {
                timestamp: "1793811751973".to_owned(),
                price_changes: vec![LevelChange {
                    asset_id: "1".to_owned(),
                    price: "0.018".to_owned(),
                    size: "1100".to_owned(),
                    side: OrderSide::Buy,
                }],
            }),
        })
        .unwrap();
        drop(tx);
        engine.run(rx).await.unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();

        let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
            panic!("expected a snapshot first");
        };
        let snapshot = SnapshotDomain::try_from_decoder(decoded).unwrap();
        assert_eq!(snapshot.symbol, "1");
        assert_eq!(snapshot.received_at, received_at);
        assert_eq!(
            snapshot.exchange_timestamp.timestamp_millis(),
            1_793_811_735_726
        );
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.bids[1].price_level, 18_000);
        assert_eq!(snapshot.bids[1].quantity, 1_254_570_000);
        assert_eq!(snapshot.asks[0].price_level, 19_000);

        let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
            panic!("expected a price level update second");
        };
        let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
        assert_eq!(update.symbol, "1");
        assert_eq!(update.side, Side::Buy);
        assert_eq!(update.price_level, 18_000);
        assert_eq!(update.quantity, 1_100_000_000);
        assert_eq!(update.backtest_timestamp, received_at);
        assert_eq!(
            update.exchange_timestamp.timestamp_millis(),
            1_793_811_751_973
        );

        assert!(reader.read_msg().unwrap().is_none());
    }
}
