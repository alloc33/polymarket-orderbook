//! Owns every order book and the single SBE writer.

use std::{
    collections::{HashMap, hash_map::Entry},
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    book::{self, LevelUpdate, OrderBook},
    polymarket::ws::{Book, Event, PriceChange},
    util::{
        AlignedSbeWriter,
        dtos::challenge_orderbook::{
            PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
            SnapshotDomain,
        },
    },
};

// The template reader can only read 64 MiB chunks.
pub const CHUNK_SIZE: usize = 64 << 20;

pub enum Inbound {
    Event {
        received_at: DateTime<Utc>,
        event: Event,
    },
    Forget(String),
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
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        // If the writer fails, the whole process must stop rather than keep sockets open.
        let _stop_everything_when_done = shutdown.drop_guard();
        let mut snapshots = tokio::time::interval_at(
            Instant::now() + self.snapshot_interval,
            self.snapshot_interval,
        );
        snapshots.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // Live data first; the snapshot timer only runs when the inbox is empty.
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
        match inbound {
            Inbound::Event {
                received_at,
                event: Event::Book(book),
            } => self.book(book, received_at),
            Inbound::Event {
                received_at,
                event: Event::PriceChange(change),
            } => self.price_change(&change, received_at),
            Inbound::Event {
                event: Event::Other,
                ..
            } => Ok(()),
            Inbound::Forget(asset) => {
                self.books.remove(&asset);
                Ok(())
            }
        }
    }

    fn book(&mut self, book: Book, received_at: DateTime<Utc>) -> anyhow::Result<()> {
        let order_book = match OrderBook::try_from(&book) {
            Ok(order_book) => order_book,
            Err(error) => {
                warn!(%error, asset = %book.asset_id, "ignored book");
                return Ok(());
            }
        };
        self.writer
            .write_msg(&snapshot(&book.asset_id, &order_book, received_at))?;
        self.books.insert(book.asset_id, order_book);
        Ok(())
    }

    fn price_change(
        &mut self,
        change: &PriceChange,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let exchange_ts = match book::parse_timestamp(&change.timestamp) {
            Ok(exchange_ts) => exchange_ts,
            Err(error) => {
                warn!(%error, "ignored price change");
                return Ok(());
            }
        };
        for level in &change.price_changes {
            let order_book = match self.books.entry(level.asset_id.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                // Polymarket sends no book for an empty book, so the first change of an
                // asset starts from empty. The empty snapshot gives readers a starting state.
                Entry::Vacant(entry) => {
                    let order_book = entry.insert(OrderBook::empty(exchange_ts));
                    self.writer
                        .write_msg(&snapshot(&level.asset_id, order_book, received_at))?;
                    order_book
                }
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
                received_at,
                exchange_ts,
            ))?;
        }
        Ok(())
    }

    // Nothing changes while a round is written: the engine is single-threaded and
    // queued events keep the receive time they were stamped with.
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

    enum Written {
        Snapshot(SnapshotDomain),
        Update(PriceLevelUpdateDomain),
    }

    async fn write_and_read_back(inbound: Vec<Inbound>) -> Vec<Written> {
        let file = tempfile::NamedTempFile::new().unwrap();
        let writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        for message in inbound {
            tx.send(message).unwrap();
        }
        drop(tx);
        Engine::new(writer, Duration::from_secs(3600))
            .run(rx, CancellationToken::new())
            .await
            .unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let mut written = Vec::new();
        while let Some(message) = reader.read_msg().unwrap() {
            written.push(match message {
                AnyMessage::Snapshot(decoded) => {
                    Written::Snapshot(SnapshotDomain::try_from_decoder(decoded).unwrap())
                }
                AnyMessage::PriceLevelUpdate(decoded) => {
                    Written::Update(PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap())
                }
                _ => panic!("unexpected message"),
            });
        }
        written
    }

    fn received_at() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_793_811_735_800).unwrap()
    }

    fn level(price: &str, size: &str) -> Level {
        Level {
            price: price.to_owned(),
            size: size.to_owned(),
        }
    }

    fn book() -> Inbound {
        Inbound::Event {
            received_at: received_at(),
            event: Event::Book(Book {
                asset_id: "1".to_owned(),
                timestamp: "1793811735726".to_owned(),
                bids: vec![level("0.017", "800"), level("0.018", "1254.57")],
                asks: vec![level("0.019", "5884.04")],
            }),
        }
    }

    fn price_change(asset_id: &str, price: &str, size: &str, side: OrderSide) -> Inbound {
        Inbound::Event {
            received_at: received_at(),
            event: Event::PriceChange(PriceChange {
                timestamp: "1793811751973".to_owned(),
                price_changes: vec![LevelChange {
                    asset_id: asset_id.to_owned(),
                    price: price.to_owned(),
                    size: size.to_owned(),
                    side,
                }],
            }),
        }
    }

    #[tokio::test]
    async fn book_is_written_as_a_snapshot() {
        let written = write_and_read_back(vec![book()]).await;

        let [Written::Snapshot(snapshot)] = written.as_slice() else {
            panic!("expected exactly one snapshot");
        };
        assert_eq!(snapshot.symbol, "1");
        assert_eq!(snapshot.received_at, received_at());
        assert_eq!(
            snapshot.exchange_timestamp.timestamp_millis(),
            1_793_811_735_726
        );
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.bids[1].price_level, 18_000);
        assert_eq!(snapshot.bids[1].quantity, 1_254_570_000);
        assert_eq!(snapshot.asks[0].price_level, 19_000);
    }

    #[tokio::test]
    async fn price_change_is_written_as_a_level_update() {
        let written = write_and_read_back(vec![
            book(),
            price_change("1", "0.018", "1100", OrderSide::Buy),
        ])
        .await;

        let [Written::Snapshot(_), Written::Update(update)] = written.as_slice() else {
            panic!("expected a snapshot followed by one update");
        };
        assert_eq!(update.symbol, "1");
        assert_eq!(update.side, Side::Buy);
        assert_eq!(update.price_level, 18_000);
        assert_eq!(update.quantity, 1_100_000_000);
        assert_eq!(update.backtest_timestamp, received_at());
        assert_eq!(
            update.exchange_timestamp.timestamp_millis(),
            1_793_811_751_973
        );
    }

    #[tokio::test]
    async fn first_price_change_of_unknown_asset_opens_an_empty_book() {
        let written =
            write_and_read_back(vec![price_change("7", "0.42", "25", OrderSide::Sell)]).await;

        let [Written::Snapshot(snapshot), Written::Update(update)] = written.as_slice() else {
            panic!("expected an empty snapshot followed by one update");
        };
        assert_eq!(snapshot.symbol, "7");
        assert_eq!(snapshot.bids, []);
        assert_eq!(snapshot.asks, []);
        assert_eq!(
            snapshot.exchange_timestamp.timestamp_millis(),
            1_793_811_751_973
        );
        assert_eq!(update.side, Side::Sell);
        assert_eq!(update.price_level, 420_000);
        assert_eq!(update.quantity, 25_000_000);
    }

    #[tokio::test]
    async fn forgotten_asset_is_not_written_again() {
        let written = write_and_read_back(vec![
            book(),
            Inbound::Forget("1".to_owned()),
            price_change("1", "0.018", "1100", OrderSide::Buy),
        ])
        .await;

        let [
            Written::Snapshot(_),
            Written::Snapshot(reopened),
            Written::Update(_),
        ] = written.as_slice()
        else {
            panic!("expected the book to be reopened empty after forget");
        };
        assert_eq!(reopened.bids, []);
    }
}
