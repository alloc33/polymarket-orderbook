# polymarket-orderbook

Order book collector for the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge).
It subscribes to every live Polymarket asset, keeps an order book per asset and
writes every change plus a full snapshot every minute, in the SBE format from the
challenge template.

The task names Kalshi, with Polymarket as the fallback for people who cannot open a
Kalshi account. Kalshi does not serve my country, so this is built on Polymarket.
Polymarket's market data is public: no account, no API key. The task's MVE exclusion
is a Kalshi concept; Polymarket's multi-outcome events are plain binary markets and
need no special handling.

## Running

```sh
cargo run --release > book.sbe
```

Logs go to stderr, data goes to stdout. Ctrl-C closes every websocket and the file.

```
--output <PATH>              where to write (default /dev/stdout)
--snapshot-interval <SECS>   full snapshot of every book (default 60)
--discovery-interval <SECS>  re-list live assets, subscribe new, drop closed (default 300)
--assets-per-socket <N>      assets per websocket (default 100)
--sockets-per-group <N>      redundant websockets per group of assets (default 2)
--limit <N>                  only the first N assets, for trying things out
```

The full universe is about 470,000 assets in about 4,700 groups, each on two
websockets, so the open file limit has to allow that (`ulimit -n 65536`). Listing the assets takes three to
eight minutes depending on how Polymarket's API feels that day; sockets start as soon
as the first page arrives.

To look at a file:

```sh
cargo run --example dump -- book.sbe
```

Data only reaches stdout when a 64 MiB chunk is full, or at shutdown. That is how
the template's reader works (it reads fixed chunks), not something to fix here.

## How it works

```
Gamma REST ──► collector ──► balancer ──► connection tasks (two per asset group)
                  │                                │ events + receive time + socket id
                  │ which sockets carry an asset   ▼
                  └──────────────────────────► engine ──► AlignedSbeWriter ──► stdout
```

Only the `polymarket` module knows the exchange. It implements the `Exchange` trait
(`exchange.rs`): list the live assets, connect, build subscribe and unsubscribe
messages, heartbeat, and decode frames into `event::Snapshot` / `event::Update` in
fixed point. The connection and the collector are generic over that trait, and
everything from the event onwards never sees an exchange type. A second exchange is a
second implementation of the trait. The trait has no state because Polymarket needs
none; an exchange that signs its handshake or tracks server-side subscription ids, as
Kalshi does, would take `&self` and keep them there.

- `polymarket/rest.rs` lists live assets page by page. An asset is one outcome token;
  every market has two. Inactive placeholder markets are skipped.
- `polymarket/ws.rs` connects, builds commands, and decodes the exchange's `book` and
  `price_change` messages into events.
- `fixed.rs` turns decimal strings into the schema's fixed-point integers.
- `balancer.rs` decides which group an asset goes to: a group with spare room,
  otherwise a new one every 100 assets.
- `connection.rs` runs one websocket: subscribe, heartbeat every 10 s, stamp each
  frame with the time it arrived, decode, forward. If the connect, a write or the feed
  stalls for 30 s it reconnects with backoff and resubscribes, and tells the engine it
  went down.
- `engine.rs` owns the books and the writer, on its own thread. Every socket of a group
  keeps its own copy of each book; only the current source's changes are written. A
  snapshot event becomes a snapshot, an update a price level update, and every minute
  it writes a snapshot of every book.
- `collector.rs` ties it together and, every five minutes, re-lists the assets to
  subscribe new ones and unsubscribe the ones that stopped trading.

## Decisions worth knowing

A symbol is Polymarket's token id, the key the websocket uses, so there is no mapping
table. The two tokens of a market are exact mirrors; both are subscribed because the
task asks for every symbol, though one per market would halve the sockets and lose
nothing.

100 assets per socket comes from measuring: with 100 the initial books arrive within a
second, with 500 they trickle in over half a minute, with 2,000 they never arrive.

A `price_change` carries the new total at a level, not a delta, so one missed message
cannot corrupt the others. Every subscribed asset gets a `book` first; if a change ever
came before it, the engine starts from an empty book and writes an empty snapshot, so
every stream begins with a snapshot.

A `price_change` is per market and carries both tokens' lines, and after a reconnect a
socket can still deliver messages for an asset that has moved elsewhere. So the
collector tells the engine which sockets carry each asset, and the engine ignores the
rest. One rule covers duplicates and late messages.

Every group of assets runs on two websockets. Both keep books; one is the source. A
socket counts as alive while frames arrive, heartbeat replies included, so a quiet
market never looks like a dead socket. If the source misses its heartbeats for 15 s,
or reports that it went down, the next event from the other socket makes it the
source, provided it holds a real book, and the engine writes a snapshot from that
book before continuing with its updates, because the two books can differ by whatever
the stalled socket missed. 15 s is half the socket's own stall detection, so the
output keeps flowing while the stalled socket reconnects. The cost is twice the
sockets and twice the books in memory.

The engine is one thread with no locks, so output order is total and the minute
snapshot is a consistent cut. Replay the file in file order; sorting by receive time
would move an in-flight event across a snapshot. Sockets feed an unbounded channel,
because a socket read that blocks on the writer delays its PING and gets it dropped.
Each snapshot round logs the worst receive-to-write lag since the last one; at full
size it stays under half a second.

On Ctrl-C the sockets get 15 s to close politely (5 s each for the handshake), anything
still wedged is aborted, then the engine flushes the last chunk and exits.

Numbers from a 17 minute full run on 2026-11-07 with two sockets per group: 475,266
assets on 9,506 sockets; 3.1 M snapshots and 12.8 M level updates written and read
back; worst receive-to-write lag per minute under 600 ms; 1.9 GB of memory. Doubling
the sockets has a visible price at this size: Polymarket throttles that many connects
from one address, so startup takes about ten minutes of stalled and retried connects,
and the engine switches source tens of thousands of times while sockets come and go.
Once settled it switches about a hundred times a minute. The output never stopped and
the process exited cleanly, which is the point of the extension. With one socket per
group the same run connects in four minutes and sees a handful of stalls. Logs are not
in the repo; the numbers vary with Polymarket's load.

## Not done

- A book that became empty while its socket was reconnecting stays stale until its next
  change, because no `book` is sent for an empty book. A REST `GET /book` on reconnect
  would fix it.
- New markets show up within about nine minutes (five-minute interval plus a listing).
  Polymarket's `new_market` push event would be faster.
- Polymarket includes a `hash` with every book. If its derivation is reproducible
  client-side, checking it after each update would catch divergence; the code does not
  look at it.
- One output file, no rotation. A crash loses the chunk being buffered (up to 64 MiB);
  flushing more often is not free, because the format pads every flush to a full chunk.
- From the task page's extensions: per-asset activity monitoring with resubscription,
  periodic full reconnects, and latency experiments.

## Tests

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Tests cover the pure parts: number and timestamp parsing, book updates, wire message
decoding including batches with a bad element, group placement, the socket ownership
rule, source switching on a paused clock, and a write-then-read-back of the engine
through the template reader. Network
behaviour was checked by running against the live feed at full size. `src/util`,
`src/side.rs` and `sbe_schemas` are the template, unchanged; `src/util` is excluded
from rustfmt so it stays that way.
