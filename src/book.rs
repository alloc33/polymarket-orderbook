use std::collections::BTreeMap;

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};

use crate::{
    Side,
    polymarket::ws::{Book, Level, LevelChange, OrderSide},
};

const DECIMALS: u32 = 6;
pub const SCALE: i64 = 10_i64.pow(DECIMALS);

#[derive(Debug)]
pub struct OrderBook {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub exchange_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelUpdate {
    pub side: Side,
    pub price: i64,
    pub quantity: i64,
}

impl OrderBook {
    pub fn empty(exchange_ts: DateTime<Utc>) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            exchange_ts,
        }
    }

    pub fn apply(
        &mut self,
        change: &LevelChange,
        exchange_ts: DateTime<Utc>,
    ) -> anyhow::Result<LevelUpdate> {
        let side = Side::from(change.side);
        let price = parse_fixed(&change.price)?;
        let quantity = parse_fixed(&change.size)?;

        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        self.exchange_ts = exchange_ts;

        Ok(LevelUpdate {
            side,
            price,
            quantity,
        })
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }
}

impl TryFrom<&Book> for OrderBook {
    type Error = anyhow::Error;

    fn try_from(book: &Book) -> anyhow::Result<Self> {
        Ok(Self {
            bids: parse_levels(&book.bids)?,
            asks: parse_levels(&book.asks)?,
            exchange_ts: parse_timestamp(&book.timestamp)?,
        })
    }
}

impl From<OrderSide> for Side {
    fn from(side: OrderSide) -> Self {
        match side {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

fn parse_levels(levels: &[Level]) -> anyhow::Result<BTreeMap<i64, i64>> {
    levels
        .iter()
        .map(|level| Ok((parse_fixed(&level.price)?, parse_fixed(&level.size)?)))
        .collect()
}

pub fn parse_fixed(text: &str) -> anyhow::Result<i64> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let decimals = u32::try_from(fraction.len())?;
    ensure!(
        decimals <= DECIMALS,
        "more than {DECIMALS} decimals: {text}"
    );

    let whole: i64 = whole
        .parse::<u64>()
        .with_context(|| format!("invalid number: {text}"))?
        .try_into()?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        let digits: u32 = fraction
            .parse()
            .with_context(|| format!("invalid number: {text}"))?;
        i64::from(digits) * 10_i64.pow(DECIMALS - decimals)
    };

    whole
        .checked_mul(SCALE)
        .and_then(|whole| whole.checked_add(fraction))
        .with_context(|| format!("out of range: {text}"))
}

pub fn parse_timestamp(millis: &str) -> anyhow::Result<DateTime<Utc>> {
    let millis: i64 = millis
        .parse()
        .with_context(|| format!("invalid timestamp: {millis}"))?;
    DateTime::from_timestamp_millis(millis)
        .with_context(|| format!("timestamp out of range: {millis}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: &str, size: &str) -> Level {
        Level {
            price: price.to_owned(),
            size: size.to_owned(),
        }
    }

    fn change(price: &str, size: &str, side: OrderSide) -> LevelChange {
        LevelChange {
            asset_id: "1".to_owned(),
            price: price.to_owned(),
            size: size.to_owned(),
            side,
        }
    }

    fn book() -> OrderBook {
        OrderBook::try_from(&Book {
            asset_id: "1".to_owned(),
            timestamp: "1793811735726".to_owned(),
            bids: vec![level("0.017", "800"), level("0.018", "1254.57")],
            asks: vec![level("0.019", "5884.04"), level("0.025", "400")],
        })
        .unwrap()
    }

    #[test]
    fn parses_fixed_point() {
        assert_eq!(parse_fixed("0.018").unwrap(), 18_000);
        assert_eq!(parse_fixed("1254.57").unwrap(), 1_254_570_000);
        assert_eq!(parse_fixed("8275").unwrap(), 8_275_000_000);
        assert_eq!(parse_fixed("0").unwrap(), 0);
        assert_eq!(parse_fixed("1").unwrap(), SCALE);
        assert_eq!(parse_fixed("0.000001").unwrap(), 1);
    }

    #[test]
    fn rejects_malformed_numbers() {
        for text in ["", "abc", "-1", "1.2345678", "1.x", ".5"] {
            assert!(parse_fixed(text).is_err(), "{text:?} should be rejected");
        }
    }

    #[test]
    fn parses_millisecond_timestamps() {
        let ts = parse_timestamp("1793811735726").unwrap();

        assert_eq!(ts.timestamp_millis(), 1_793_811_735_726);
        assert!(parse_timestamp("soon").is_err());
    }

    #[test]
    fn book_message_becomes_sorted_levels() {
        let book = book();

        assert_eq!(
            book.bids,
            BTreeMap::from([(17_000, 800_000_000), (18_000, 1_254_570_000)])
        );
        assert_eq!(
            book.asks,
            BTreeMap::from([(19_000, 5_884_040_000), (25_000, 400_000_000)])
        );
        assert_eq!(book.exchange_ts.timestamp_millis(), 1_793_811_735_726);
        assert_eq!(book.best_bid(), Some(18_000));
        assert_eq!(book.best_ask(), Some(19_000));
    }

    #[test]
    fn price_change_sets_absolute_quantity() {
        let mut book = book();
        let ts = parse_timestamp("1793811751973").unwrap();

        let update = book
            .apply(&change("0.018", "1100", OrderSide::Buy), ts)
            .unwrap();

        assert_eq!(
            update,
            LevelUpdate {
                side: Side::Buy,
                price: 18_000,
                quantity: 1_100_000_000
            }
        );
        assert_eq!(book.bids[&18_000], 1_100_000_000);
        assert_eq!(book.exchange_ts, ts);
    }

    #[test]
    fn price_change_adds_and_removes_levels() {
        let mut book = book();
        let ts = parse_timestamp("1793811751973").unwrap();

        book.apply(&change("0.021", "50", OrderSide::Sell), ts)
            .unwrap();
        assert_eq!(book.asks[&21_000], 50_000_000);

        book.apply(&change("0.019", "0", OrderSide::Sell), ts)
            .unwrap();
        assert!(!book.asks.contains_key(&19_000));
        assert_eq!(book.best_ask(), Some(21_000));
    }

    #[test]
    fn empty_sides_have_no_best_price() {
        let book = OrderBook::try_from(&Book {
            asset_id: "1".to_owned(),
            timestamp: "0".to_owned(),
            bids: vec![],
            asks: vec![],
        })
        .unwrap();

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }
}
