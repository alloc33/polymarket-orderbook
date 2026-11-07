# Latency notes

What was measured on 2026-11-06 and 2026-11-07 from one machine in Bangkok against
Polymarket's production endpoints, and what was not tried. All numbers come from the
runs described in the README; `cargo run --example dump -- file.sbe` prints the lag
percentiles from any output file.

## Two different latencies

The output carries two timestamps per update: the exchange's own (`exchange_timestamp`)
and the moment the frame reached this process (`backtest_timestamp`). Their difference
is how late the exchange delivered the change; it is the number a backtest cares about,
and nothing in this process can shorten it.

The engine separately logs `max_lag_ms` every minute: the worst arrival-to-engine lag,
from the frame reaching the socket task to the engine handling the event. Encoding into
the chunk buffer follows immediately; the chunk itself hits the file when it is full.
That is the part this process controls.

## Numbers

Full universe, two sockets per group (9,504 sockets), 15 minutes including the
startup:

- exchange to receive: p50 84 ms, p90 656 ms, p99 16.1 s, max 251 s. A later run the
  same morning saw p50 312 ms and p90 5.5 s under heavier exchange load. The two
  timestamps come from two clocks; the local one is NTP-synced (macOS default) but its
  offset was not measured, so treat the medians as approximate.
- arrival-to-engine lag: under 400 ms every minute, typically 200 to 300 ms

Capped runs on 100 to 300 assets on a single socket: exchange to receive around 30 ms
median. Connect times: 200 sockets opened in parallel all connected, the slowest in
3.6 s; a single socket connects in 0.7 s.

## What the numbers say

The long tail comes from the exchange side. During the first ten minutes of a
9,500-socket startup Polymarket throttles new connections and buffers delivery for
sockets it has accepted; updates then arrive seconds to minutes after their exchange
timestamp. Once the sockets settle the tail shrinks. This is what the redundant socket
per group covers: whichever socket the exchange is currently serving well becomes the
source.

The pipeline itself adds little: JSON decoding happens in the socket tasks in
parallel, the engine is one thread that does a map insert and an SBE encode per update,
and its worst minute stays under 400 ms even while writing a 475,000-book snapshot
round.

## Not tried

- Racing several connections and keeping the fastest: viable at small scale, where
  connect times spread from 0.7 to 3.6 s. At this scale my guess is that every extra
  connection costs more in exchange-side throttling than it could win, but that was
  not measured.
- Changing the TCP source port, and connecting right after an exchange maintenance
  window: both need a controlled comparison over days, which this submission did not
  have.
