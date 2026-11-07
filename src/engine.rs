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
// The exchange can deliver the same update to two sockets a minute apart under load,
// so one idle round proves nothing; three in a row with the twin active do.
const REPAIR_AFTER_ROUNDS: u8 = 3;
// A feed still waiting for its book after a repair is asked again each round. The cap
// keeps a format change at the exchange, which breaks every feed at once, from turning
// into a resubscribe storm of hundreds of thousands a minute.
const RETRIES_PER_ROUND: usize = 1_000;

// Asked of the collector when one socket of a group should resubscribe an asset.
pub struct Repair {
    pub asset: String,
    pub socket: usize,
}

pub enum Inbound {
    Assign {
        asset: String,
        sockets: Vec<usize>,
    },
    Forget(String),
    Alive(usize),
    Down(usize),
    Broken {
        asset: String,
        socket: usize,
    },
    Event {
        socket: usize,
        received_at: DateTime<Utc>,
        event: Event,
    },
}

#[derive(Clone, Copy, Default)]
struct SocketState {
    last_frame: Option<Instant>,
    session: u32,
}

impl SocketState {
    fn stale(self) -> bool {
        self.last_frame.is_none_or(|at| at.elapsed() > SWITCH_AFTER)
    }
}

#[derive(Default)]
struct Sockets(HashMap<usize, SocketState>);

impl Sockets {
    fn state(&self, socket: usize) -> SocketState {
        self.0.get(&socket).copied().unwrap_or_default()
    }

    // The quiet clock of a new socket starts now, not at its first frame.
    fn start(&mut self, socket: usize) {
        self.0.entry(socket).or_insert(SocketState {
            last_frame: Some(Instant::now()),
            session: 0,
        });
    }

    fn alive(&mut self, socket: usize) {
        self.0.entry(socket).or_default().last_frame = Some(Instant::now());
    }

    // A reconnect starts a new session: books from the old one no longer count.
    fn down(&mut self, socket: usize) {
        let state = self.0.entry(socket).or_default();
        state.last_frame = None;
        state.session += 1;
    }
}

struct Feed {
    socket: usize,
    book: Option<OrderBook>,
    session: u32,
    events: u32,
    resync: bool,
}

impl Feed {
    fn fresh(&self, sockets: &Sockets) -> Option<&OrderBook> {
        self.book
            .as_ref()
            .filter(|_| self.session == sockets.state(self.socket).session)
    }
}

// Every socket of the group keeps its own book; only the source's changes are written.
struct Subscription {
    source: usize,
    feeds: Vec<Feed>,
    idle_rounds: u8,
}

impl Subscription {
    fn feed_mut(&mut self, socket: usize) -> Option<&mut Feed> {
        self.feeds.iter_mut().find(|feed| feed.socket == socket)
    }

    fn fresh_book(&self, socket: usize, sockets: &Sockets) -> Option<&OrderBook> {
        self.feeds
            .iter()
            .find(|feed| feed.socket == socket)
            .and_then(|feed| feed.fresh(sockets))
    }

    fn source_book(&self, sockets: &Sockets) -> Option<&OrderBook> {
        self.fresh_book(self.source, sockets)
    }

    fn twin_with_book(&self, sockets: &Sockets) -> Option<usize> {
        self.feeds
            .iter()
            .find(|feed| feed.socket != self.source && feed.fresh(sockets).is_some())
            .map(|feed| feed.socket)
    }

    // A source with no events for several rounds while another feed keeps delivering
    // is a subscription that died quietly. Returns the live socket to switch to.
    fn dead_source(&mut self, sockets: &Sockets) -> Option<usize> {
        let source = self.source;
        let idle = self
            .feeds
            .iter()
            .any(|feed| feed.socket == source && feed.events == 0);
        let live = self
            .feeds
            .iter()
            .find(|feed| feed.socket != source && feed.events > 0 && feed.fresh(sockets).is_some());
        let Some(live) = live.filter(|_| idle) else {
            self.idle_rounds = 0;
            return None;
        };
        self.idle_rounds += 1;
        (self.idle_rounds >= REPAIR_AFTER_ROUNDS).then_some(live.socket)
    }

    fn stuck_sockets(&self) -> impl Iterator<Item = usize> + '_ {
        self.feeds
            .iter()
            .filter(|feed| feed.resync)
            .map(|feed| feed.socket)
    }

    // Returns whether this was a failover from a working source. At startup the twin's
    // book simply arriving first is not one.
    fn switch_to(&mut self, asset: &str, to: usize, sockets: &Sockets) -> bool {
        let had_book = self.source_book(sockets).is_some();
        debug!(asset, from = self.source, to, had_book, "switched source");
        self.source = to;
        self.idle_rounds = 0;
        had_book
    }
}

pub struct Engine {
    subscriptions: HashMap<String, Subscription>,
    sockets: Sockets,
    writer: AlignedSbeWriter,
    repairs: mpsc::UnboundedSender<Repair>,
    snapshot_interval: Duration,
    max_lag: TimeDelta,
    switches: usize,
}

impl Engine {
    pub fn new(
        writer: AlignedSbeWriter,
        repairs: mpsc::UnboundedSender<Repair>,
        snapshot_interval: Duration,
    ) -> Self {
        Self {
            subscriptions: HashMap::new(),
            sockets: Sockets::default(),
            writer,
            repairs,
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
                self.assign(asset, &sockets);
                Ok(())
            }
            Inbound::Forget(asset) => {
                self.subscriptions.remove(&asset);
                Ok(())
            }
            Inbound::Alive(socket) => {
                self.sockets.alive(socket);
                Ok(())
            }
            Inbound::Down(socket) => {
                self.sockets.down(socket);
                Ok(())
            }
            Inbound::Broken { asset, socket } => self.broken(&asset, socket),
            Inbound::Event {
                socket,
                received_at,
                event,
            } => {
                self.max_lag = self.max_lag.max(Utc::now() - received_at);
                self.sockets.alive(socket);
                match event {
                    Event::Snapshot(snapshot) => self.snapshot(socket, &snapshot, received_at),
                    Event::Update(update) => self.update(socket, &update, received_at),
                }
            }
        }
    }

    fn assign(&mut self, asset: String, sockets: &[usize]) {
        for &socket in sockets {
            self.sockets.start(socket);
        }
        let feeds = sockets
            .iter()
            .map(|&socket| Feed {
                socket,
                book: None,
                session: 0,
                events: 0,
                resync: false,
            })
            .collect();
        self.subscriptions.insert(
            asset,
            Subscription {
                source: sockets[0],
                feeds,
                idle_rounds: 0,
            },
        );
    }

    // A line that could not be decoded leaves that socket's book unreliable: drop it,
    // ignore the feed until a fresh book arrives, ask for a resubscribe once, and let a
    // twin with a real book take over meanwhile.
    fn broken(&mut self, asset: &str, socket: usize) -> anyhow::Result<()> {
        let Some(subscription) = self.subscriptions.get_mut(asset) else {
            return Ok(());
        };
        let Some(feed) = subscription.feed_mut(socket) else {
            return Ok(());
        };
        if feed.resync {
            return Ok(());
        }
        feed.book = None;
        feed.resync = true;
        // The collector going away means the process is already shutting down.
        let _ = self.repairs.send(Repair {
            asset: asset.to_owned(),
            socket,
        });
        self.adopt_twin(asset, Utc::now())
    }

    fn snapshot(
        &mut self,
        socket: usize,
        snapshot: &Snapshot,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let session = self.sockets.state(socket).session;
        let Some(subscription) = self.subscriptions.get_mut(&snapshot.asset) else {
            return Ok(());
        };
        let source = subscription.source;
        let Some(feed) = subscription.feed_mut(socket) else {
            return Ok(());
        };
        let book = OrderBook::from(snapshot);
        if source == socket {
            self.writer
                .write_msg(&encode_snapshot(&snapshot.asset, &book, received_at))?;
        }
        feed.events += 1;
        feed.session = session;
        feed.resync = false;
        feed.book = Some(book);
        if source != socket
            && (subscription.source_book(&self.sockets).is_none()
                || self.sockets.state(source).stale())
        {
            self.switch_source(&snapshot.asset, socket, received_at)?;
        }
        Ok(())
    }

    fn update(
        &mut self,
        socket: usize,
        update: &Update,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let session = self.sockets.state(socket).session;
        let Some(subscription) = self.subscriptions.get_mut(&update.asset) else {
            return Ok(());
        };
        let source = subscription.source;
        let Some(feed) = subscription.feed_mut(socket) else {
            return Ok(());
        };
        if feed.resync || feed.session != session {
            return Ok(());
        }
        feed.events += 1;
        let Some(book) = &mut feed.book else {
            if source == socket {
                return self.open_source(socket, update, received_at);
            }
            return Ok(());
        };
        book.apply(update);
        if source == socket {
            return self.writer.write_msg(&encode_update(update, received_at));
        }
        if self.sockets.state(source).stale() {
            self.switch_source(&update.asset, socket, received_at)?;
        }
        Ok(())
    }

    // The exchange sends a book for every subscribed asset, so an update before it is
    // unexpected. A twin that already holds a real book beats starting from empty.
    fn open_source(
        &mut self,
        socket: usize,
        update: &Update,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(subscription) = self.subscriptions.get_mut(&update.asset) else {
            return Ok(());
        };
        if let Some(twin) = subscription.twin_with_book(&self.sockets) {
            return self.switch_source(&update.asset, twin, received_at);
        }
        warn!(asset = %update.asset, "update before snapshot");
        let mut book = OrderBook::empty(update.exchange_ts);
        self.writer
            .write_msg(&encode_snapshot(&update.asset, &book, received_at))?;
        book.apply(update);
        self.writer.write_msg(&encode_update(update, received_at))?;
        if let Some(feed) = subscription.feed_mut(socket) {
            feed.book = Some(book);
        }
        Ok(())
    }

    fn switch_source(
        &mut self,
        asset: &str,
        to: usize,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(subscription) = self.subscriptions.get_mut(asset) else {
            return Ok(());
        };
        self.switches += usize::from(subscription.switch_to(asset, to, &self.sockets));
        if let Some(book) = subscription.source_book(&self.sockets) {
            self.writer
                .write_msg(&encode_snapshot(asset, book, received_at))?;
        }
        Ok(())
    }

    fn adopt_twin(&mut self, asset: &str, received_at: DateTime<Utc>) -> anyhow::Result<()> {
        let Some(subscription) = self.subscriptions.get(asset) else {
            return Ok(());
        };
        if subscription.source_book(&self.sockets).is_some() {
            return Ok(());
        }
        match subscription.twin_with_book(&self.sockets) {
            Some(twin) => self.switch_source(asset, twin, received_at),
            None => Ok(()),
        }
    }

    // Nothing changes while a round is written: the engine is single-threaded and
    // queued events keep the receive time they were stamped with.
    fn snapshot_all(&mut self) -> anyhow::Result<()> {
        let received_at = Utc::now();
        let mut books = 0;
        let mut repairs = 0;
        let mut retries = 0;
        for (symbol, subscription) in &mut self.subscriptions {
            if let Some(live) = subscription.dead_source(&self.sockets) {
                let _ = self.repairs.send(Repair {
                    asset: symbol.clone(),
                    socket: subscription.source,
                });
                repairs += 1;
                self.switches += usize::from(subscription.switch_to(symbol, live, &self.sockets));
            } else if subscription.source_book(&self.sockets).is_none()
                && let Some(twin) = subscription.twin_with_book(&self.sockets)
            {
                self.switches += usize::from(subscription.switch_to(symbol, twin, &self.sockets));
            }
            for socket in subscription
                .stuck_sockets()
                .take(RETRIES_PER_ROUND - retries)
            {
                let _ = self.repairs.send(Repair {
                    asset: symbol.clone(),
                    socket,
                });
                retries += 1;
            }
            for feed in &mut subscription.feeds {
                feed.events = 0;
            }
            if let Some(book) = subscription.source_book(&self.sockets) {
                self.writer
                    .write_msg(&encode_snapshot(symbol, book, received_at))?;
                books += 1;
            }
        }
        info!(
            books,
            max_lag_ms = self.max_lag.num_milliseconds(),
            switches = self.switches,
            repairs,
            retries,
            "snapshot round"
        );
        self.max_lag = TimeDelta::zero();
        self.switches = 0;
        Ok(())
    }
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
    const MINUTE: Duration = Duration::from_secs(60);

    enum Written {
        Snapshot(SnapshotDomain),
        Update(PriceLevelUpdateDomain),
    }

    struct Harness {
        file: tempfile::NamedTempFile,
        tx: mpsc::UnboundedSender<Inbound>,
        repairs: mpsc::UnboundedReceiver<Repair>,
        run: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl Harness {
        fn start(snapshot_interval: Duration) -> Self {
            let file = tempfile::NamedTempFile::new().unwrap();
            let writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE).unwrap();
            let (repairs_tx, repairs) = mpsc::unbounded_channel();
            let (tx, rx) = mpsc::unbounded_channel();
            let engine = Engine::new(writer, repairs_tx, snapshot_interval);
            let run = tokio::spawn(engine.run(rx, CancellationToken::new()));
            Self {
                file,
                tx,
                repairs,
                run,
            }
        }

        fn send(&self, inbound: Inbound) {
            self.tx.send(inbound).unwrap();
        }

        async fn settle(&self) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        async fn finish(self) -> Vec<Written> {
            drop(self.tx);
            self.run.await.unwrap().unwrap();
            let mut reader = AlignedSbeReader::<OrderbookSchema>::open(self.file.path()).unwrap();
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
    }

    async fn write_and_read_back(inbound: Vec<Inbound>) -> Vec<Written> {
        let harness = Harness::start(Duration::from_secs(3600));
        for message in inbound {
            harness.send(message);
        }
        harness.finish().await
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

    fn bid_update(socket: usize) -> Inbound {
        price_change(socket, 998_000, 1_100_000_000, Side::Buy)
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
        let written = write_and_read_back(vec![assign(&[0]), book(0), bid_update(0)]).await;

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
        let written = write_and_read_back(vec![assign(&[0]), book(1), bid_update(1)]).await;

        assert!(written.is_empty());
    }

    #[tokio::test]
    async fn forgotten_asset_is_not_written_again() {
        let written = write_and_read_back(vec![
            assign(&[0]),
            book(0),
            Inbound::Forget(ASSET.to_owned()),
            bid_update(0),
        ])
        .await;

        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }

    #[tokio::test]
    async fn redundant_socket_stays_silent_while_the_source_delivers() {
        let written =
            write_and_read_back(vec![assign(&[0, 1]), book(0), book(1), bid_update(1)]).await;

        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }

    #[tokio::test]
    async fn update_on_a_bookless_source_switches_to_a_twin_with_a_book() {
        let written = write_and_read_back(vec![assign(&[0, 1]), book(1), bid_update(0)]).await;

        let [Written::Snapshot(snapshot)] = written.as_slice() else {
            panic!("expected one snapshot from the twin and nothing else");
        };
        assert_eq!(snapshot.bids.len(), 2);
    }

    #[tokio::test]
    async fn source_switches_at_once_when_its_socket_goes_down() {
        let written = write_and_read_back(vec![
            assign(&[0, 1]),
            book(0),
            book(1),
            Inbound::Down(0),
            bid_update(1),
        ])
        .await;

        let [Written::Snapshot(_), Written::Snapshot(switched)] = written.as_slice() else {
            panic!("expected the original snapshot and one from the new source");
        };
        assert_eq!(switched.bids[1].quantity, 1_100_000_000);
    }

    #[tokio::test]
    async fn reconnected_socket_counts_only_after_a_fresh_book() {
        let written = write_and_read_back(vec![
            assign(&[0, 1]),
            book(0),
            book(1),
            Inbound::Down(0),
            Inbound::Down(1),
            bid_update(1),
            book(1),
        ])
        .await;

        let [Written::Snapshot(_), Written::Snapshot(fresh)] = written.as_slice() else {
            panic!("expected the stale update ignored and a switch on the fresh book");
        };
        assert_eq!(fresh.bids[1].quantity, 287_197_360_000);
    }

    #[tokio::test]
    async fn broken_line_drops_the_book_asks_for_repair_and_adopts_the_twin() {
        let mut harness = Harness::start(Duration::from_secs(3600));
        harness.send(assign(&[0, 1]));
        harness.send(book(0));
        harness.send(book(1));
        harness.send(Inbound::Broken {
            asset: ASSET.to_owned(),
            socket: 0,
        });
        harness.settle().await;

        let repair = harness.repairs.try_recv().unwrap();
        assert_eq!((repair.asset.as_str(), repair.socket), (ASSET, 0));
        let written = harness.finish().await;
        assert!(matches!(
            written.as_slice(),
            [Written::Snapshot(_), Written::Snapshot(_)]
        ));
    }

    #[tokio::test]
    async fn broken_line_without_a_twin_waits_for_the_fresh_book() {
        let mut harness = Harness::start(Duration::from_secs(3600));
        harness.send(assign(&[0]));
        harness.send(book(0));
        harness.send(Inbound::Broken {
            asset: ASSET.to_owned(),
            socket: 0,
        });
        harness.send(Inbound::Broken {
            asset: ASSET.to_owned(),
            socket: 0,
        });
        harness.send(bid_update(0));
        harness.send(book(0));
        harness.settle().await;

        assert!(harness.repairs.try_recv().is_ok());
        assert!(harness.repairs.try_recv().is_err());
        let written = harness.finish().await;
        assert!(matches!(
            written.as_slice(),
            [Written::Snapshot(_), Written::Snapshot(_)]
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_resync_is_asked_again_each_round_until_the_book_arrives() {
        let mut harness = Harness::start(MINUTE);
        harness.send(assign(&[0]));
        harness.send(Inbound::Broken {
            asset: ASSET.to_owned(),
            socket: 0,
        });
        harness.settle().await;
        assert!(harness.repairs.try_recv().is_ok());
        tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
        harness.settle().await;
        assert!(harness.repairs.try_recv().is_ok());

        harness.send(book(0));
        harness.settle().await;
        tokio::time::advance(MINUTE).await;
        harness.settle().await;
        assert!(harness.repairs.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_are_capped_per_round() {
        let mut harness = Harness::start(MINUTE);
        for i in 0..=RETRIES_PER_ROUND {
            let asset = format!("asset-{i}");
            harness.send(Inbound::Assign {
                asset: asset.clone(),
                sockets: vec![0],
            });
            harness.send(Inbound::Broken { asset, socket: 0 });
        }
        harness.settle().await;
        while harness.repairs.try_recv().is_ok() {}
        tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
        harness.settle().await;

        let retried = std::iter::from_fn(|| harness.repairs.try_recv().ok()).count();
        assert_eq!(retried, RETRIES_PER_ROUND);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_replies_keep_a_quiet_source() {
        let harness = Harness::start(Duration::from_secs(3600));
        harness.send(assign(&[0, 1]));
        harness.send(book(0));
        harness.send(book(1));
        harness.settle().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        harness.send(Inbound::Alive(0));
        harness.settle().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        harness.send(bid_update(1));

        let written = harness.finish().await;
        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }

    #[tokio::test(start_paused = true)]
    async fn redundant_socket_without_a_book_never_becomes_source() {
        let harness = Harness::start(Duration::from_secs(3600));
        harness.send(assign(&[0, 1]));
        harness.send(book(0));
        harness.settle().await;
        tokio::time::advance(SWITCH_AFTER + Duration::from_secs(1)).await;
        harness.send(bid_update(1));

        let written = harness.finish().await;
        assert!(matches!(written.as_slice(), [Written::Snapshot(_)]));
    }

    #[tokio::test(start_paused = true)]
    async fn source_switches_to_the_socket_that_keeps_delivering() {
        let harness = Harness::start(Duration::from_secs(3600));
        harness.send(assign(&[0, 1]));
        harness.send(book(0));
        harness.send(book(1));
        harness.settle().await;
        tokio::time::advance(SWITCH_AFTER + Duration::from_secs(1)).await;
        harness.send(bid_update(1));

        let written = harness.finish().await;
        let [Written::Snapshot(_), Written::Snapshot(switched)] = written.as_slice() else {
            panic!("expected the original snapshot and one from the new source");
        };
        assert_eq!(switched.bids[1].quantity, 1_100_000_000);
    }

    #[tokio::test(start_paused = true)]
    async fn round_falls_back_to_a_twin_when_the_source_lost_its_book() {
        let harness = Harness::start(MINUTE);
        harness.send(assign(&[0, 1]));
        harness.send(book(0));
        harness.send(book(1));
        harness.send(Inbound::Down(0));
        harness.settle().await;
        tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
        harness.settle().await;

        let written = harness.finish().await;
        let [Written::Snapshot(_), Written::Snapshot(round)] = written.as_slice() else {
            panic!("expected the round written from the twin's book");
        };
        assert_eq!(round.symbol, ASSET);
    }

    #[tokio::test(start_paused = true)]
    async fn silent_source_is_replaced_and_repaired_after_three_rounds() {
        let mut harness = Harness::start(MINUTE);
        harness.send(assign(&[0, 1]));
        harness.send(book(0));
        harness.send(book(1));
        harness.settle().await;
        tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
        harness.settle().await;
        for round in 1..=REPAIR_AFTER_ROUNDS {
            harness.send(Inbound::Alive(0));
            harness.send(bid_update(1));
            harness.settle().await;
            tokio::time::advance(MINUTE).await;
            harness.settle().await;
            if round < REPAIR_AFTER_ROUNDS {
                assert!(harness.repairs.try_recv().is_err());
            }
        }

        let repair = harness.repairs.try_recv().unwrap();
        assert_eq!((repair.asset.as_str(), repair.socket), (ASSET, 0));
        let written = harness.finish().await;
        let Some(Written::Snapshot(last)) = written.last() else {
            panic!("expected the round snapshot from the new source");
        };
        assert_eq!(last.bids[1].quantity, 1_100_000_000);
    }
}
