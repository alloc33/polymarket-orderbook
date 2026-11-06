//! Owns every order book and the single SBE writer.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    book::OrderBook,
    event::{Event, Snapshot, Update},
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
// A source socket that sent no frame for this long, not even a heartbeat reply, is
// replaced by a redundant one that did. Shorter than the socket's own 30 s stall
// detection, so the output keeps flowing while the stalled socket reconnects.
const SWITCH_AFTER: Duration = Duration::from_secs(15);

pub enum Inbound {
    Assign {
        asset: String,
        sockets: Vec<usize>,
    },
    Forget(String),
    Alive(usize),
    Down(usize),
    Event {
        socket: usize,
        received_at: DateTime<Utc>,
        event: Event,
    },
}

struct Feed {
    socket: usize,
    book: Option<OrderBook>,
}

// Every socket of the group keeps its own book; only the source's changes are written.
struct Subscription {
    source: usize,
    feeds: Vec<Feed>,
}

impl Subscription {
    fn book(&self) -> Option<&OrderBook> {
        self.feeds
            .iter()
            .find(|feed| feed.socket == self.source)
            .and_then(|feed| feed.book.as_ref())
    }
}

fn feed(feeds: &mut [Feed], socket: usize) -> Option<&mut Feed> {
    feeds.iter_mut().find(|feed| feed.socket == socket)
}

pub struct Engine {
    subscriptions: HashMap<String, Subscription>,
    last_event: HashMap<usize, Instant>,
    writer: AlignedSbeWriter,
    snapshot_interval: Duration,
    max_lag: TimeDelta,
    switches: usize,
}

impl Engine {
    pub fn new(writer: AlignedSbeWriter, snapshot_interval: Duration) -> Self {
        Self {
            subscriptions: HashMap::new(),
            last_event: HashMap::new(),
            writer,
            snapshot_interval,
            max_lag: TimeDelta::zero(),
            switches: 0,
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
            Inbound::Assign { asset, sockets } => {
                // The quiet clock of a new socket starts now, not at its first event.
                for &socket in &sockets {
                    self.last_event.entry(socket).or_insert_with(Instant::now);
                }
                let feeds = sockets
                    .iter()
                    .map(|&socket| Feed { socket, book: None })
                    .collect();
                self.subscriptions.insert(
                    asset,
                    Subscription {
                        source: sockets[0],
                        feeds,
                    },
                );
                Ok(())
            }
            Inbound::Forget(asset) => {
                self.subscriptions.remove(&asset);
                Ok(())
            }
            Inbound::Alive(socket) => {
                self.last_event.insert(socket, Instant::now());
                Ok(())
            }
            Inbound::Down(socket) => {
                self.last_event.remove(&socket);
                Ok(())
            }
            Inbound::Event {
                socket,
                received_at,
                event,
            } => {
                self.max_lag = self.max_lag.max(Utc::now() - received_at);
                self.last_event.insert(socket, Instant::now());
                match event {
                    Event::Snapshot(snapshot) => self.snapshot(socket, &snapshot, received_at),
                    Event::Update(update) => self.update(socket, &update, received_at),
                }
            }
        }
    }

    fn snapshot(
        &mut self,
        socket: usize,
        snapshot: &Snapshot,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(subscription) = self.subscriptions.get_mut(&snapshot.asset) else {
            return Ok(());
        };
        let Some(feed) = feed(&mut subscription.feeds, socket) else {
            return Ok(());
        };
        let order_book = feed.book.insert(OrderBook::from(snapshot));
        if subscription.source != socket {
            if !stale(&self.last_event, subscription.source) {
                return Ok(());
            }
            debug!(asset = %snapshot.asset, from = subscription.source, to = socket, "switched source");
            self.switches += 1;
            subscription.source = socket;
        }
        self.writer
            .write_msg(&encode_snapshot(&snapshot.asset, order_book, received_at))?;
        Ok(())
    }

    fn update(
        &mut self,
        socket: usize,
        update: &Update,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(subscription) = self.subscriptions.get_mut(&update.asset) else {
            return Ok(());
        };
        let Some(feed) = feed(&mut subscription.feeds, socket) else {
            return Ok(());
        };
        if subscription.source == socket {
            // The exchange sends a book for every subscribed asset, so an update before it
            // is unexpected. Starting from an empty book keeps the stream snapshot-first.
            if feed.book.is_none() {
                warn!(asset = %update.asset, "update before snapshot");
                let empty = OrderBook::empty(update.exchange_ts);
                self.writer
                    .write_msg(&encode_snapshot(&update.asset, &empty, received_at))?;
                feed.book = Some(empty);
            }
        }
        // A redundant socket only counts once it has a real book to switch to.
        let Some(order_book) = &mut feed.book else {
            return Ok(());
        };
        order_book.apply(update);
        if subscription.source == socket {
            self.writer.write_msg(&encode_update(update, received_at))?;
        } else if stale(&self.last_event, subscription.source) {
            debug!(asset = %update.asset, from = subscription.source, to = socket, "switched source");
            self.switches += 1;
            subscription.source = socket;
            self.writer
                .write_msg(&encode_snapshot(&update.asset, order_book, received_at))?;
        }
        Ok(())
    }

    // Nothing changes while a round is written: the engine is single-threaded and
    // queued events keep the receive time they were stamped with.
    fn snapshot_all(&mut self) -> anyhow::Result<()> {
        let received_at = Utc::now();
        let mut books = 0;
        for (symbol, subscription) in &self.subscriptions {
            if let Some(book) = subscription.book() {
                self.writer
                    .write_msg(&encode_snapshot(symbol, book, received_at))?;
                books += 1;
            }
        }
        info!(
            books,
            max_lag_ms = self.max_lag.num_milliseconds(),
            switches = self.switches,
            "snapshot round"
        );
        self.max_lag = TimeDelta::zero();
        self.switches = 0;
        Ok(())
    }
}

fn stale(last_event: &HashMap<usize, Instant>, socket: usize) -> bool {
    last_event
        .get(&socket)
        .is_none_or(|at| at.elapsed() > SWITCH_AFTER)
}

fn encode_snapshot(symbol: &str, book: &OrderBook, received_at: DateTime<Utc>) -> SnapshotDomain {
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

fn encode_update(update: &Update, received_at: DateTime<Utc>) -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        backtest_timestamp: received_at,
        exchange_timestamp: update.exchange_ts,
        price_level: update.price,
        quantity: update.quantity,
        side: update.side,
        symbol: update.asset.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Side,
        event::Level,
        util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage},
    };

    const ASSET: &str =
        "21910945789202551310737182019481519202352824012676720876277298283641159588352";

    enum Written {
        Snapshot(SnapshotDomain),
        Update(PriceLevelUpdateDomain),
    }

    fn engine(file: &tempfile::NamedTempFile) -> Engine {
        let writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE).unwrap();
        Engine::new(writer, Duration::from_secs(3600))
    }

    fn read_back(file: &tempfile::NamedTempFile) -> Vec<Written> {
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

    async fn write_and_read_back(inbound: Vec<Inbound>) -> Vec<Written> {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        for message in inbound {
            tx.send(message).unwrap();
        }
        drop(tx);
        engine(&file)
            .run(rx, CancellationToken::new())
            .await
            .unwrap();
        read_back(&file)
    }

    fn received_at() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_793_977_813_040).unwrap()
    }

    fn assign(sockets: &[usize]) -> Inbound {
        Inbound::Assign {
            asset: ASSET.to_owned(),
            sockets: sockets.to_vec(),
        }
    }

    fn level(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    fn book(socket: usize) -> Inbound {
        Inbound::Event {
            socket,
            received_at: received_at(),
            event: Event::Snapshot(Snapshot {
                asset: ASSET.to_owned(),
                exchange_ts: DateTime::from_timestamp_millis(1_793_977_813_000).unwrap(),
                bids: vec![
                    level(997_000, 105_486_430_000),
                    level(998_000, 287_197_360_000),
                ],
                asks: vec![level(999_000, 1_369_896_930_000)],
            }),
        }
    }

    fn price_change(socket: usize, price: i64, quantity: i64, side: Side) -> Inbound {
        Inbound::Event {
            socket,
            received_at: received_at(),
            event: Event::Update(Update {
                asset: ASSET.to_owned(),
                exchange_ts: DateTime::from_timestamp_millis(1_793_977_816_442).unwrap(),
                side,
                price,
                quantity,
            }),
        }
    }

    #[tokio::test]
    async fn book_is_written_as_a_snapshot() {
        let written = write_and_read_back(vec![assign(&[0]), book(0)]).await;

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
            assign(&[0]),
            book(0),
            price_change(0, 998_000, 1_100_000_000, Side::Buy),
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
            assign(&[0]),
            price_change(0, 420_000, 25_000_000, Side::Sell),
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
    async fn messages_from_a_socket_outside_the_group_are_ignored() {
        let written = write_and_read_back(vec![
            assign(&[0]),
            book(1),
            price_change(1, 998_000, 1_100_000_000, Side::Buy),
        ])
        .await;

        assert!(written.is_empty());
    }

    #[tokio::test]
    async fn forgotten_asset_is_not_written_again() {
        let written = write_and_read_back(vec![
            assign(&[0]),
            book(0),
            Inbound::Forget(ASSET.to_owned()),
            price_change(0, 998_000, 1_100_000_000, Side::Buy),
        ])
        .await;

        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }

    #[tokio::test]
    async fn redundant_socket_stays_silent_while_the_source_delivers() {
        let written = write_and_read_back(vec![
            assign(&[0, 1]),
            book(0),
            book(1),
            price_change(1, 998_000, 1_100_000_000, Side::Buy),
        ])
        .await;

        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_replies_keep_a_quiet_source() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let run = tokio::spawn(engine(&file).run(rx, CancellationToken::new()));

        tx.send(assign(&[0, 1])).unwrap();
        tx.send(book(0)).unwrap();
        tx.send(book(1)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tx.send(Inbound::Alive(0)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tx.send(price_change(1, 998_000, 1_100_000_000, Side::Buy))
            .unwrap();
        drop(tx);
        run.await.unwrap().unwrap();

        assert!(matches!(
            read_back(&file).as_slice(),
            [Written::Snapshot(_)]
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn redundant_socket_without_a_book_never_becomes_source() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let run = tokio::spawn(engine(&file).run(rx, CancellationToken::new()));

        tx.send(assign(&[0, 1])).unwrap();
        tx.send(book(0)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(SWITCH_AFTER + Duration::from_secs(1)).await;
        tx.send(price_change(1, 998_000, 1_100_000_000, Side::Buy))
            .unwrap();
        drop(tx);
        run.await.unwrap().unwrap();

        assert!(matches!(
            read_back(&file).as_slice(),
            [Written::Snapshot(_)]
        ));
    }

    #[tokio::test]
    async fn source_switches_at_once_when_its_socket_goes_down() {
        let written = write_and_read_back(vec![
            assign(&[0, 1]),
            book(0),
            book(1),
            Inbound::Down(0),
            price_change(1, 998_000, 1_100_000_000, Side::Buy),
        ])
        .await;

        let [Written::Snapshot(_), Written::Snapshot(switched)] = written.as_slice() else {
            panic!("expected the original snapshot and one from the new source");
        };
        assert_eq!(switched.bids[1].quantity, 1_100_000_000);
    }

    #[tokio::test(start_paused = true)]
    async fn source_switches_to_the_socket_that_keeps_delivering() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let run = tokio::spawn(engine(&file).run(rx, CancellationToken::new()));

        tx.send(assign(&[0, 1])).unwrap();
        tx.send(book(0)).unwrap();
        tx.send(book(1)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(SWITCH_AFTER + Duration::from_secs(1)).await;
        tx.send(price_change(1, 998_000, 1_100_000_000, Side::Buy))
            .unwrap();
        drop(tx);
        run.await.unwrap().unwrap();

        let written = read_back(&file);
        let [Written::Snapshot(_), Written::Snapshot(switched)] = written.as_slice() else {
            panic!("expected the original snapshot and one from the new source");
        };
        assert_eq!(switched.bids[1].price_level, 998_000);
        assert_eq!(switched.bids[1].quantity, 1_100_000_000);
    }
}
