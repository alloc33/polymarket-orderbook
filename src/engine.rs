//! Owns every order book and the single SBE writer.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
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
    Assign {
        asset: String,
        socket: usize,
    },
    Forget(String),
    Event {
        socket: usize,
        received_at: DateTime<Utc>,
        event: Event,
    },
}

struct Subscription {
    socket: usize,
    book: Option<OrderBook>,
}

pub struct Engine {
    subscriptions: HashMap<String, Subscription>,
    writer: AlignedSbeWriter,
    snapshot_interval: Duration,
    max_lag: TimeDelta,
}

impl Engine {
    pub fn new(writer: AlignedSbeWriter, snapshot_interval: Duration) -> Self {
        Self {
            subscriptions: HashMap::new(),
            writer,
            snapshot_interval,
            max_lag: TimeDelta::zero(),
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

        loop {
            tokio::select! {
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
            Inbound::Assign { asset, socket } => {
                self.subscriptions
                    .insert(asset, Subscription { socket, book: None });
                Ok(())
            }
            Inbound::Forget(asset) => {
                self.subscriptions.remove(&asset);
                Ok(())
            }
            Inbound::Event {
                socket,
                received_at,
                event,
            } => {
                self.max_lag = self.max_lag.max(Utc::now() - received_at);
                match event {
                    Event::Book(book) => self.book(socket, &book, received_at),
                    Event::PriceChange(change) => self.price_change(socket, &change, received_at),
                    Event::Other => Ok(()),
                }
            }
        }
    }

    fn book(
        &mut self,
        socket: usize,
        book: &Book,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(subscription) = owned(&mut self.subscriptions, &book.asset_id, socket) else {
            return Ok(());
        };
        match OrderBook::try_from(book) {
            Ok(order_book) => {
                self.writer
                    .write_msg(&snapshot(&book.asset_id, &order_book, received_at))?;
                subscription.book = Some(order_book);
            }
            Err(error) => {
                warn!(%error, asset = %book.asset_id, "dropped a book that could not be read");
                subscription.book = None;
            }
        }
        Ok(())
    }

    fn price_change(
        &mut self,
        socket: usize,
        change: &PriceChange,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let exchange_ts = match book::parse_timestamp(&change.timestamp) {
            Ok(exchange_ts) => exchange_ts,
            Err(error) => {
                warn!(%error, "ignored a price change with an unreadable timestamp");
                return Ok(());
            }
        };
        for level in &change.price_changes {
            let Some(subscription) = owned(&mut self.subscriptions, &level.asset_id, socket) else {
                continue;
            };
            // Polymarket sends a book for every subscribed asset, so a change before the book
            // is unexpected. Starting from an empty snapshot keeps every stream snapshot-first.
            if subscription.book.is_none() {
                warn!(asset = %level.asset_id, "price change before book");
                let empty = OrderBook::empty(exchange_ts);
                self.writer
                    .write_msg(&snapshot(&level.asset_id, &empty, received_at))?;
                subscription.book = Some(empty);
            }
            let Some(order_book) = &mut subscription.book else {
                continue;
            };
            match order_book.apply(level, exchange_ts) {
                Ok(update) => self.writer.write_msg(&level_update(
                    &level.asset_id,
                    update,
                    received_at,
                    exchange_ts,
                ))?,
                Err(error) => {
                    warn!(%error, asset = %level.asset_id, "dropped a book after an unreadable level");
                    subscription.book = None;
                }
            }
        }
        Ok(())
    }

    // Nothing changes while a round is written: the engine is single-threaded and
    // queued events keep the receive time they were stamped with.
    fn snapshot_all(&mut self) -> anyhow::Result<()> {
        let received_at = Utc::now();
        let mut books = 0;
        for (symbol, subscription) in &self.subscriptions {
            if let Some(book) = &subscription.book {
                self.writer
                    .write_msg(&snapshot(symbol, book, received_at))?;
                books += 1;
            }
        }
        info!(
            books,
            max_lag_ms = self.max_lag.num_milliseconds(),
            "snapshot round"
        );
        self.max_lag = TimeDelta::zero();
        Ok(())
    }
}

// Only the socket an asset is assigned to may touch its book. Anything else is a late
// message from a socket that no longer owns the asset.
fn owned<'a>(
    subscriptions: &'a mut HashMap<String, Subscription>,
    asset: &str,
    socket: usize,
) -> Option<&'a mut Subscription> {
    subscriptions
        .get_mut(asset)
        .filter(|subscription| subscription.socket == socket)
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

    const ASSET: &str =
        "21910945789202551310737182019481519202352824012676720876277298283641159588352";

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
        DateTime::from_timestamp_millis(1_793_977_813_040).unwrap()
    }

    fn assign(socket: usize) -> Inbound {
        Inbound::Assign {
            asset: ASSET.to_owned(),
            socket,
        }
    }

    fn level(price: &str, size: &str) -> Level {
        Level {
            price: price.to_owned(),
            size: size.to_owned(),
        }
    }

    fn book(socket: usize) -> Inbound {
        Inbound::Event {
            socket,
            received_at: received_at(),
            event: Event::Book(Book {
                asset_id: ASSET.to_owned(),
                timestamp: "1793977813000".to_owned(),
                bids: vec![level("0.997", "105486.43"), level("0.998", "287197.36")],
                asks: vec![level("0.999", "1369896.93")],
            }),
        }
    }

    fn price_change(socket: usize, price: &str, size: &str, side: OrderSide) -> Inbound {
        Inbound::Event {
            socket,
            received_at: received_at(),
            event: Event::PriceChange(PriceChange {
                timestamp: "1793977816442".to_owned(),
                price_changes: vec![LevelChange {
                    asset_id: ASSET.to_owned(),
                    price: price.to_owned(),
                    size: size.to_owned(),
                    side,
                }],
            }),
        }
    }

    #[tokio::test]
    async fn book_is_written_as_a_snapshot() {
        let written = write_and_read_back(vec![assign(0), book(0)]).await;

        let [Written::Snapshot(snapshot)] = written.as_slice() else {
            panic!("expected exactly one snapshot");
        };
        assert_eq!(snapshot.symbol, ASSET);
        assert_eq!(snapshot.received_at, received_at());
        assert_eq!(
            snapshot.exchange_timestamp.timestamp_millis(),
            1_793_977_813_000
        );
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.bids[1].price_level, 998_000);
        assert_eq!(snapshot.bids[1].quantity, 287_197_360_000);
        assert_eq!(snapshot.asks[0].price_level, 999_000);
    }

    #[tokio::test]
    async fn price_change_is_written_as_a_level_update() {
        let written = write_and_read_back(vec![
            assign(0),
            book(0),
            price_change(0, "0.998", "1100", OrderSide::Buy),
        ])
        .await;

        let [Written::Snapshot(_), Written::Update(update)] = written.as_slice() else {
            panic!("expected a snapshot followed by one update");
        };
        assert_eq!(update.symbol, ASSET);
        assert_eq!(update.side, Side::Buy);
        assert_eq!(update.price_level, 998_000);
        assert_eq!(update.quantity, 1_100_000_000);
        assert_eq!(update.backtest_timestamp, received_at());
        assert_eq!(
            update.exchange_timestamp.timestamp_millis(),
            1_793_977_816_442
        );
    }

    #[tokio::test]
    async fn change_before_book_starts_from_an_empty_snapshot() {
        let written = write_and_read_back(vec![
            assign(0),
            price_change(0, "0.42", "25", OrderSide::Sell),
        ])
        .await;

        let [Written::Snapshot(snapshot), Written::Update(update)] = written.as_slice() else {
            panic!("expected an empty snapshot followed by one update");
        };
        assert_eq!(snapshot.bids, []);
        assert_eq!(snapshot.asks, []);
        assert_eq!(update.price_level, 420_000);
        assert_eq!(update.quantity, 25_000_000);
    }

    #[tokio::test]
    async fn messages_from_a_socket_that_does_not_own_the_asset_are_ignored() {
        let written = write_and_read_back(vec![
            assign(0),
            book(1),
            price_change(1, "0.998", "1100", OrderSide::Buy),
        ])
        .await;

        assert!(written.is_empty());
    }

    #[tokio::test]
    async fn forgotten_asset_is_not_written_again() {
        let written = write_and_read_back(vec![
            assign(0),
            book(0),
            Inbound::Forget(ASSET.to_owned()),
            price_change(0, "0.998", "1100", OrderSide::Buy),
        ])
        .await;

        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }
}
