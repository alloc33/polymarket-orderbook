//! In-memory order book: one sorted map per side, in fixed point.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::{
    Side,
    event::{Level, Snapshot, Update},
};

#[derive(Debug)]
pub struct OrderBook {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub exchange_ts: DateTime<Utc>,
}

impl OrderBook {
    pub fn empty(exchange_ts: DateTime<Utc>) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            exchange_ts,
        }
    }

    // An update carries the new total at the level, not a delta; zero removes it.
    pub fn apply(&mut self, update: &Update) {
        let levels = match update.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if update.quantity == 0 {
            levels.remove(&update.price);
        } else {
            levels.insert(update.price, update.quantity);
        }
        self.exchange_ts = update.exchange_ts;
    }
}

impl From<&Snapshot> for OrderBook {
    fn from(snapshot: &Snapshot) -> Self {
        Self {
            bids: levels(&snapshot.bids),
            asks: levels(&snapshot.asks),
            exchange_ts: snapshot.exchange_ts,
        }
    }
}

fn levels(levels: &[Level]) -> BTreeMap<i64, i64> {
    levels
        .iter()
        .map(|level| (level.price, level.quantity))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    fn level(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    fn update(side: Side, price: i64, quantity: i64) -> Update {
        Update {
            asset: "1".to_owned(),
            exchange_ts: ts(1_793_977_816_442),
            side,
            price,
            quantity,
        }
    }

    fn book() -> OrderBook {
        OrderBook::from(&Snapshot {
            asset: "1".to_owned(),
            exchange_ts: ts(1_793_977_813_000),
            bids: vec![
                level(997_000, 105_486_430_000),
                level(998_000, 287_197_360_000),
            ],
            asks: vec![level(999_000, 1_369_896_930_000)],
        })
    }

    #[test]
    fn snapshot_becomes_sorted_levels() {
        let book = book();

        assert_eq!(
            book.bids,
            BTreeMap::from([(997_000, 105_486_430_000), (998_000, 287_197_360_000)])
        );
        assert_eq!(book.asks, BTreeMap::from([(999_000, 1_369_896_930_000)]));
        assert_eq!(book.exchange_ts, ts(1_793_977_813_000));
    }

    #[test]
    fn update_sets_the_absolute_quantity() {
        let mut book = book();

        book.apply(&update(Side::Buy, 998_000, 1_100_000_000));

        assert_eq!(book.bids[&998_000], 1_100_000_000);
        assert_eq!(book.exchange_ts, ts(1_793_977_816_442));
    }

    #[test]
    fn update_adds_and_removes_levels() {
        let mut book = book();

        book.apply(&update(Side::Sell, 1_000_000, 50_000_000));
        assert_eq!(book.asks[&1_000_000], 50_000_000);

        book.apply(&update(Side::Sell, 999_000, 0));
        assert!(!book.asks.contains_key(&999_000));
    }
}
