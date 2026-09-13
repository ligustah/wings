# Folding channel transport into the Store

A design proposal, converged in discussion 2026-09-14. No code yet.

## The claim

Today `flow` has two storage-shaped seams: `Store` (per-thread history) and
`ChannelHost` (cross-run channel transport). They are entangled at runtime —
a channel send commits inside the sending thread's history transaction so the
send record and the event that justifies it come home together — but that
coupling is hand-wired in the **wings** layer (`job.txns` / `coordOutputs` /
`coordStore`), invisible in the `flow` interfaces.

The proposal collapses them. A channel is not a special transport; it is just
another **named durable stream** a thread writes to inside its transaction and
anyone reads back by name. The store becomes the dumbest possible thing —
named streams plus atomic transactions — and everything that made channels
look special (exclusivity, replication, backpressure, retirement) moves to the
layer that already owns it: the execution engine.

## What each layer owns

    engine        placement, exclusivity (one writer per stream),
                  replication/routing, backpressure, retirement
      │
    high store    logical transactions: which events co-commit,
                  CommitInterval coalescing
      │
    low store     named streams: atomic batch append, read, drop

The cut between the two store layers is the resolution of "must the store be
tx-aware?": the **low** level is not (it exposes only an atomic multi-stream
apply), the **high** level is (it groups a thread's writes into one logical
transaction and coalesces many of them onto the low level). Both positions
from the discussion were right, at different layers.

## Proposed interfaces

### Low-level store

Named streams, no notion of transactions, threads, runs, writers, or machines.

```go
// LowStore is a set of append-only named streams.
type LowStore interface {
    // Read returns up to n records of a stream from offset, oldest first,
    // each with its offset. Fewer than n means the stream ends there.
    Read(ctx context.Context, name string, offset int64, n int) ([]RecordAt, error)

    // Apply lands a batch of appends atomically across streams: every record
    // in the batch is durable, or none is.
    Apply(ctx context.Context, batch []Append) error

    // Drop discards a stream.
    Drop(ctx context.Context, name string) error
}

type Append   struct{ Stream string; Record []byte }
type RecordAt struct{ Record []byte; Offset int64 }
```

`Apply` is the one non-negotiable primitive: without an atomic cross-stream
batch at the bottom, no higher layer can manufacture the co-commit guarantee.

### High-level store

Logical per-thread transactions over a `LowStore`.

```go
// Store hands out a transaction per thread and reads/drops streams by name.
type Store interface {
    Begin(ctx context.Context, run, thread string) (Tx, error)
    Read(ctx context.Context, name string, offset int64, n int) ([]RecordAt, error)
    Drop(ctx context.Context, name string) error
}

// Tx is one thread's transaction: appends to any named stream commit together.
type Tx interface {
    Append(ctx context.Context, stream string, ev *protos.Event) error
    Commit(ctx context.Context) error
    Abort(ctx context.Context) error
}
```

A thread's history stream and every channel stream it writes are appended
through the same `Tx`, so the send-record-with-its-event atomicity that wings
hand-wires today becomes a property of the interface: one thread, one tx, many
streams. `Commit`'s coalescing (fold many logical txns into fewer `Apply`
calls for the CommitInterval) lives here, not in callers.

## What disappears

- `ChannelHost`, `ChannelLink`, `ChannelValueReader`, `ChannelRetirer`,
  `readerAnnouncer` — a channel is a named stream; there is no transport seam.
- `WithChannelHost` — gone; channels need only a `Store`.
- `job.txns` / `coordOutputs` / `coordStore` as a separate mechanism — this
  *is* the high-level `Tx`, promoted from a wings side-car to the store layer.
- The local-vs-shared channel split's remaining machinery — every channel is
  the same named stream; there is no in-process fast path to carve out.

Channel *semantics* that are genuinely the engine's move up, not away:
backpressure (engine reads the consume stream and folds counts), reader-push
vs. coordinator-read (replication), and retirement (dropping a channel's
streams when its creator is done) become engine operations over named streams.

## Current → proposed, per implementation

| today | becomes |
|---|---|
| `streamStore` (dsclient) | `LowStore` over dsclient; `Store`/`Tx` layered above, generic |
| `MemStore` | `LowStore` over maps; inherits `Store`/`Tx` unchanged |
| `historyStore` (worker) | drops its bespoke txn plumbing; a `Tx` over the node's `LowStore` |
| `coordStore` + `job.txns` + `coordOutputs` | the generic high-level `Store`/`Tx` — no wings-specific store |
| `inspectionStore` | reads named streams via `Read`; unchanged in spirit |
| `clusterChannels` / `nodeChannels` | deleted; channel writes are `Tx.Append(chanValues(id), …)`, reads are `Store.Read(chanValues(id), …)` |

The high-level `Tx` and its coalescing are written **once**, backend-agnostic.
A new backend implements only the dumb-but-atomic `LowStore` and inherits
transactions for free.

## Reference backends (keep the interface honest)

**durable_streams.** `LowStore.Apply` is a producer transaction spanning
several streams; `Read` is a stream read from an offset. Replication is real:
a stream is per-node, so the engine pushes a channel's value stream to the
reader's worker and pulls a writer's streams home.

**SQL.** One table:

```sql
CREATE TABLE events (
    stream  TEXT   NOT NULL,
    seq     BIGINT NOT NULL,
    payload BLOB   NOT NULL,
    PRIMARY KEY (stream, seq)
);
```

- `Read(name, offset, n)` → `SELECT payload, seq FROM events WHERE stream=? AND seq>=? ORDER BY seq LIMIT ?`
- `Drop(name)` → `DELETE FROM events WHERE stream=?`
- `Apply(batch)` / `Tx.Commit` → one DB transaction wrapping the `INSERT`s
- `seq` is the per-stream position; nothing more

On a single shared SQL database **replication vanishes**: the reader just
`SELECT`s the channel's rows. Same `Store` interface; the engine's replication
layer is simply empty. That is the proof that cross-machine was never a store
concern — only durable_streams' per-node streams made it look like one.

## Non-goals / settled points

- **No idempotency key, no `ON CONFLICT`.** Replay reads the recorded event
  and skips the send; it never re-appends. There is no duplicate to dedup.
- **No fencing / epoch / producer token in the interface.** Enforcing a single
  writer is the execution engine's job (placement / move); how a distributed
  log backstops it physically stays below the interface.
- **No machine-awareness in the store.** Read/append by name; the engine
  decides where a stream physically lives and how it is replicated.

## Open questions

- `Read` today is keyed `(run, thread)`; the proposal keys by stream `name`.
  `streamName(run, thread)` becomes the caller's concern (or a thin helper).
  Confirm nothing outside the store relies on the `(run, thread)` shape.
- `Tail` and `ListRuns`/`ListThreads` (on the concrete stores today) — keep as
  `Store` extras, or push name-enumeration into the engine?
- Migration order: introduce `Store`/`Tx` alongside the existing `Store`,
  move wings onto it, then delete `ChannelHost` — or a single cut.
