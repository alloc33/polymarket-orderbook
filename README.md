# polymarket-orderbook

Order book collector for the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge).
It subscribes to every live Polymarket asset, keeps an order book per asset and
writes every change plus a full snapshot every minute, in the SBE format from the
challenge template.

The task names Kalshi, with Polymarket as the fallback for people who cannot open a
Kalshi account. Kalshi does not serve my country, so this is built on Polymarket.
Polymarket's market data is public: no account, no API key.

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

The full universe is about 490,000 assets on about 4,900 websockets, so the open
file limit has to allow that (`ulimit -n 65536`). Listing the assets takes three to
four minutes; sockets start as soon as the first page arrives.

To look at a file:

```sh
cargo run --example dump -- book.sbe
```

Note that data only reaches stdout when a 64 MiB chunk is full, or at shutdown. That
is how the template's reader works (it reads fixed chunks), not something to fix here.

## How it works

```
Gamma REST ──► collector ──► balancer ──► connection tasks (one per websocket)
                                                   │ events + receive time
                                                   ▼
                                                engine ──► AlignedSbeWriter ──► stdout
```

- `polymarket/rest.rs` lists live assets page by page (an asset is one outcome token;
  every market has two).
- `polymarket/ws.rs` is the wire format of the websocket: subscribe commands and the
  `book` / `price_change` messages.
- `balancer.rs` decides which socket an asset goes to: a socket with spare room,
  otherwise a new one every 100 assets.
- `connection.rs` runs one websocket: subscribe, PING every 10 s, stamp each frame with
  the time it arrived, forward it. If the socket dies or goes silent for 30 s it
  reconnects and resubscribes.
- `engine.rs` is the only owner of the books and the writer. A `book` message becomes a
  snapshot, each `price_change` line becomes a price level update, and every minute it
  writes a snapshot of every book.
- `collector.rs` ties it together and, every five minutes, re-lists the assets to
  subscribe new ones and unsubscribe the ones that stopped trading.

## Decisions worth knowing

A symbol is Polymarket's token id. It is what the websocket keys every message by, so
nothing needs a mapping table. The two tokens of a market are mirror books; both are
subscribed because both trade.

100 assets per socket is measured, not guessed. With 100 the initial books arrive within
a second. With 500 they trickle in over half a minute, with 2,000 they never arrive.

Polymarket's `price_change` carries the new total at a price level, not a delta, so the
book just overwrites the level and a missed message cannot corrupt the others. It also
sends no `book` for an empty book; roughly a third of assets start that way. The first
change of such an asset opens an empty book and writes an empty snapshot first, so
every symbol's stream begins with a snapshot.

A `price_change` is per market and carries both tokens' lines. Each socket drops lines
for assets it did not subscribe; otherwise a market whose tokens sit on two sockets
would be written twice.

One engine task owns all books and the writer, so there are no locks and the output
order is total. The minute snapshot is a consistent cut: nothing changes while it is
written, and events arriving meanwhile keep the receive time they were stamped with.
Sockets send into an unbounded channel because a socket read must never block on the
writer; blocking would delay the PING and get the socket dropped, which is data loss.

Unsubscribing goes through the socket: it sends the unsubscribe frame, then tells the
engine to forget the book. Because that order is fixed on one channel, a late update can
never bring the book back.

On Ctrl-C the sockets get 15 s to close politely; the few whose peer stopped reading
get aborted, then the engine flushes the last chunk and the process exits.

Numbers from a 16 minute run on everything: 4,884 sockets, 170 reconnects, zero lost
sockets, 7.4 M snapshots and 27 M level updates written, about 1.4 GB of memory.

## Not done

- A book that became empty while its socket was reconnecting stays stale until its next
  change, because no `book` is sent for it. A REST `GET /book` on reconnect would fix it.
- New markets show up within five to eight minutes. Polymarket's `new_market` push
  event would be faster.
- One output file, no rotation.
- The extensions from the task page: redundant sockets, per-asset activity monitoring,
  a second exchange behind the same engine, latency experiments.

## Tests

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Tests cover the pure parts: number and timestamp parsing, book updates, wire message
parsing, socket placement, the asset filter, and a write-then-read-back of the engine
through the template reader. Network behaviour was checked by running against the live
feed at full size. `src/util`, `src/side.rs` and `sbe_schemas` are the template,
unchanged.
