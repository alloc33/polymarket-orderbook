//! What an exchange decoder produces: snapshots and level updates in the fixed-point
//! representation of the SBE schema. Everything after decoding works on these.

use chrono::{DateTime, Utc};

use crate::Side;

pub enum Event {
    Snapshot(Snapshot),
    Update(Update),
}

pub struct Snapshot {
    pub asset: String,
    pub exchange_ts: DateTime<Utc>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

pub struct Update {
    pub asset: String,
    pub exchange_ts: DateTime<Utc>,
    pub side: Side,
    pub price: i64,
    pub quantity: i64,
}
