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
--limit <N>                  only the first N assets, for trying things out
```

The full universe is about 470,000 assets on about 4,700 websockets, so the open
file limit has to allow that (`ulimit -n 65536`). Listing the assets takes three to
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
Gamma REST ──► collector ──► balancer ──► connection tasks (one per websocket)
                  │                                │ events + receive time + socket id
                  │ who owns which asset           ▼
                  └──────────────────────────► engine ──► AlignedSbeWriter ──► stdout
```

- `polymarket/rest.rs` lists live assets page by page. An asset is one outcome token;
  every market has two. Inactive placeholder markets are skipped.
- `polymarket/ws.rs` is the wire format of the websocket: subscribe commands and the
  `book` / `price_change` messages.
- `balancer.rs` decides which socket an asset goes to: a socket with spare room,
  otherwise a new one every 100 assets.
- `connection.rs` runs one websocket: subscribe, PING every 10 s, stamp each frame with
  the time it arrived, forward it. If the connect, a write or the feed stalls for 30 s
  it reconnects with backoff and resubscribes.
- `engine.rs` owns the books and the writer, on its own thread. A `book` message
  becomes a snapshot, each `price_change` line a price level update, and every minute
  it writes a snapshot of every book. It only accepts messages from the socket an
  asset is assigned to.
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
collector tells the engine which socket owns each asset, and the engine ignores the
rest. One rule covers duplicates and late messages.

The engine is one thread with no locks, so output order is total and the minute
snapshot is a consistent cut. Replay the file in file order; sorting by receive time
would move an in-flight event across a snapshot. Sockets feed an unbounded channel,
because a socket read that blocks on the writer delays its PING and gets it dropped.
Each snapshot round logs the worst receive-to-write lag since the last one; at full
size it stays under half a second.

On Ctrl-C the sockets get 15 s to close politely (5 s each for the handshake), anything
still wedged is aborted, then the engine flushes the last chunk and exits.

Numbers from a 22 minute full run on 2026-11-06: 473,254 assets on 4,734 sockets;
one discovery round added 130 and removed 96; about 540 reconnects, nearly all 30 s
silences; 9.4 M snapshots and 37 M level updates written and read back; 1.2 GB of
memory. Logs are not in the repo and the numbers vary with Polymarket's load.

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
- The extensions from the task page: redundant sockets, per-asset activity monitoring,
  a second exchange behind the same engine, latency experiments.

## Tests

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Tests cover the pure parts: number and timestamp parsing, book updates, wire message
parsing including batches with a bad element, socket placement, the socket ownership
rule, and a write-then-read-back of the engine through the template reader. Network
behaviour was checked by running against the live feed at full size. `src/util`,
`src/side.rs` and `sbe_schemas` are the template, unchanged; `src/util` is excluded
from rustfmt so it stays that way.
