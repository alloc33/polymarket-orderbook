//! Polymarket order book collector: lists the live assets, streams their books
//! over many websockets and writes every change as SBE with the Voluntas template.

#![warn(clippy::pedantic, clippy::cognitive_complexity)]
#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::similar_names
)]
#![deny(unsafe_code)]

pub mod balancer;
pub mod book;
pub mod collector;
pub mod connection;
pub mod engine;
pub mod event;
pub mod exchange;
pub mod fixed;
pub mod polymarket;

// The template modules are vendored as-is and keep their own lint settings.
#[allow(clippy::pedantic, clippy::nursery, unsafe_code)]
pub mod util;

#[allow(clippy::pedantic, clippy::nursery, unsafe_code)]
mod side;
pub use side::{InvalidSide, Side};
