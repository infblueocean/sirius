# Streaming Sessions

How data enters and leaves a Sirius plan fragment when the fragment is one hop of a larger,
distributed query. Five pieces: a **streaming source** (`STREAMING_SOURCE`), a **streaming
sink** (`STREAMING_SINK`), the `exec::batch_stream` both are built on, the
`exec::stream_session` router that addresses them by stream id, and the
`exec::streaming_fragment` that builds and runs one fragment's plan around them.

Sirius itself stays fragment-blind. It never learns that it is distributed, which compute node a
partition ships to, or how many nodes exist. All of that lives in the wrapper above the engine.

## The model: the repository is the queue

A `cucascade::shared_data_repository` already *is* a thread-safe queue of `data_batch`es, and it
is the owner of record the downgrade executor sweeps for spill candidates. What it lacks is the
**stream state**: who is still producing, whether "nothing right now" means *wait* or *the
stream is over*, how a starved consumer gets woken, and how a producer failure reaches the
consumer.

`exec::batch_stream` is that pairing, written once: it borrows the repository (the caller
creates it and registers it with the memory manager, so queued batches stay spillable) and owns
everything the repository lacks. Each streaming operator holds one — or, for a partitioned
sink, one per destination.

| Concern | Owned by |
|---|---|
| The queue of batches | `cucascade::shared_data_repository` |
| End-of-stream, availability, waking, producer failure | `exec::batch_stream` |

Producers push and consumers pull through the stream, which touches the repository under its own
lock. There is **no bounded channel and no channel-level backpressure** — see
[Why no backpressure](#why-no-backpressure).

Batches cross this boundary **natively**, as `cucascade::data_batch`, in whatever tier they
currently sit. Nothing is materialized to Arrow on the way in or out, so a queued batch stays
spillable (GPU → host → disk) right up until it is pulled, and `try_pull()` hands it back in its
current tier without forcing an upgrade.

## `exec::batch_stream`

**Files:** `src/include/exec/batch_stream.hpp`, `src/exec/batch_stream.cpp`

One direction of batch flow: N declared senders push into one repository; consumers pull, poll,
or block. Non-copyable and non-movable (it owns a mutex and a condition variable); hold it by
`shared_ptr` — producer threads co-own it, so the stream they push through outlives the
operator exactly as the repository already did.

```cpp
class batch_stream {
 public:
  enum class availability { HAS_DATA, WAITING, END_OF_STREAM };

  batch_stream(std::shared_ptr<cucascade::shared_data_repository> repo,
               std::set<sender_id_t> expected);

  // Producer — any thread
  [[nodiscard]] bool push(std::shared_ptr<cucascade::data_batch> batch);  // S1: false once terminal
  void close(sender_id_t sender);                                         // idempotent per sender
  void fail(std::exception_ptr error);                                    // poison; P1–P4, S2

  // Consumer
  std::shared_ptr<cucascade::data_batch> try_pull();  // S4: rethrows a pending error first
  availability classify() const;                      // S3: an errored stream is never EOS
  bool drained() const;                               // clean end only (S3)
  std::exception_ptr pending_error() const;
  void wait();                                        // S5: external threads only

  // Hooks — one slot each, fire after unlocking
  void set_on_data(std::function<void()> hook);
  void set_on_end_of_stream(std::function<void()> hook);
};
```

Four things are load-bearing. Their observable contracts are the S1–S5 labels defined at the
class declaration and cited by the call sites and tests.

**End-of-stream is a set, not a counter.** A fan-in stream — one root source fed by N remote
leaves — is over only when *all N* senders have closed. A counter cannot tell "both senders
closed once" from "one sender closed twice", so a bare `mark_done()` cannot be both idempotent
and fan-in-correct. `close(sender)` inserts into a set of closed sender ids and compares it
against the expected set. A repeat close is a genuine no-op and cannot advance a fan-in stream
on its own; an id outside the expected set is a defined error — a wiring bug, not something to
silently count. An *empty* expected set means the stream is terminal from construction, a
legitimate degenerate case.

**Admission, close, and every emptiness check share one lock (S1).** `push()` inserts into the
repository *under* the stream's lock, so a close cannot interleave: no batch is admitted after
end-of-stream, no queued batch is ever reported as end-of-stream, and a batch is registered in
the repository before `on_data` announces it. Hooks fire after unlocking, so a callback may
re-enter the scheduler safely. The one exception is `try_pull()`: its pop runs outside the
lock, so **wait-then-pull is not atomic** (S5) and a blocking consumer loop must re-check after
`wait()` returns.

**`classify()` separates "not yet" from "never" (S3).** Queued data outranks terminal: EOS is never
reported while a batch the stream already accepted is still pullable.

| terminal? | repo empty? | error pending? | `classify()` |
|---|---|---|---|
| no | no | no | `HAS_DATA` |
| no | yes | no | `WAITING` |
| yes | no | no | `HAS_DATA` |
| yes | yes | no | `END_OF_STREAM` |
| any | any | **yes** | `HAS_DATA` (P4) |

`wait()` is the blocking form (block until `classify() != WAITING`) for the external consumer
thread. Engine workers never call it.

**A producer failure is stream-wide and never a clean end.** `fail(error)` carries no sender
identity and waits for nobody. The contract is P1–P4, defined at the declaration and cited by
the tests:

- **P1 — immediate visibility.** `pending_error()` returns the failure as soon as `fail()`
  returns.
- **P2 — the first failure wins.** Later failures and clean closes never displace the original
  cause the consumer will see.
- **P3 — fail-fast terminal.** The stream ends at once rather than waiting for senders that
  will never produce anything useful now.
- **P4 — poison is data.** A pending error classifies as `HAS_DATA` even over an empty queue,
  and is never `END_OF_STREAM` or `drained()`. An errored stream ends by rethrow out of
  `try_pull()` — checked *before* the pop, so batches queued behind a failure are never handed
  out — not by a quiet clean finish that would let a failed query pass as an empty one.

## `STREAMING_SOURCE` — the input boundary

**Files:** `src/include/op/sirius_physical_streaming_source.hpp`, `src/op/sirius_physical_streaming_source.cpp`

Holds one `batch_stream` over the caller's input repository, constructed with the fragment's
expected sender set. Producers call `push(batch)`, `close_input(sender_id)`, and — when a
producer dies — `fail_input(error)`; the engine sees an ordinary source.

| Stream state | `get_next_task_hint()` |
|---|---|
| `HAS_DATA` | `READY{this}` |
| `WAITING` | `WAITING{nullptr}` |
| `END_OF_STREAM` | `std::nullopt` |

The `nullptr` producer is deliberate — there is no upstream operator for `task_creator` to
redirect the request to. `all_ports_empty()` is `stream.drained()`, so an errored stream is
never mistaken for a finished one; `get_next_task_input_data()` is `try_pull()` (one batch per
task, zero-copy, rethrowing a pending producer error ahead of anything queued); `execute()` is a
pass-through.

### The live re-arm

The engine is pull-scheduled and event-poor. A head that answers `WAITING{nullptr}` is dropped,
and the only built-in re-nomination is task completion — so a stream-fed source that starved
(open and empty) has **no completing task to wake it**. Without a live re-arm the streaming
source would only ever run when some other task happened to be in flight.

`set_pipeline()` installs both stream hooks once, and both weak-capture the pipeline rather
than `this`:

- `on_data` → `task_creator::schedule(head)`. The hook is **persistent** — every successful
  `push()` fires it, and a batch is in the repository before it fires, so no wake is lost and
  a batch that arrives after the source was dropped is still looked at. `schedule()` only
  enqueues onto the thread-safe `_task_creation_queue` — it does not re-enter the operator or
  take pipeline locks, so it is safe to fire from a foreign thread (a GPU worker mid-`sink()`,
  or the wrapper's network thread). A late schedule after `task_scheduler::drain_after_error()`
  is dropped by the existing interrupted-queue path.
- `on_end_of_stream` → `pipeline->update_pipeline_status(false)` handles the case the re-arm
  cannot: a stream that closes with **no task in flight** (an empty stream, or a late close
  after the last task completed) has nothing to call `update_pipeline_status()` for it. `false`
  (rather than the default `true`) matters — it makes `notify_downstream_pipelines()` also
  schedule this pipeline's consumers, so a late-closed stream re-arms its downstream.
  Registering the hook after the stream already ended fires it immediately, so a raced close is
  not lost.

## `STREAMING_SINK` — the output boundary

**Files:** `src/include/op/sirius_physical_streaming_sink.hpp`, `src/op/sirius_physical_streaming_sink.cpp`

A pipeline-terminal operator holding **one `batch_stream` per destination**, each over its own
output repository. `execute()` is a pass-through override — the executor runs every operator in
the chain, terminal sink included, and feeds the chain's result back into `sink()`; the base
implementation returns an empty batch list, so without the override the sink would receive
nothing and the pipeline would "succeed" with an empty output stream. `sink()` pushes each
output batch; `on_finalize_operator()` — the existing pipeline-finish hook — closes every
output stream as `PIPELINE_SENDER`, the single expected sender of all of them, which is what
makes `END_OF_STREAM` observable on each. `build_pipelines()` appends the sink to the current
pipeline (landing at `operators[0]` after the reverse); the base sink path skips the append,
which would leave the terminal operator out of the root pipeline and the source with nothing
driving it.

Consumers use `pull(i)` / `wait(i)` / `drained(i)`, plus `availability(i)` for the non-blocking
three-way classification. When the query dies, `fail_output(error)` poisons every output
stream: external consumers unblock from `wait()` and collect the rethrow from `pull()` — never
a clean end. Unlike the source the sink arms no re-arm hook: its consumer is an external thread
in `wait()`, not an engine task that needs re-nominating.

### Partition fan-out

A sink can expose **N output streams**, one per destination. `sink()` GPU-hash-partitions each
batch by the `partition_spec`'s key columns (reusing `gpu_partition_impl::hash_partition`, the
same kernel the `PARTITION` operator uses) and pushes slice *i* into stream *i*; empty slices
are skipped rather than published as zero-row batches a consumer would pull and discard. A slow
receiver's backlog accumulates in its own repository — spillable by the downgrade executor —
without head-of-line-blocking the others. The single-destination sink is simply the N = 1 case,
and keeps the identity-preserving no-partition path.

```cpp
struct partition_spec {
  std::vector<int> key_columns;                 // hashed to pick a destination
  std::vector<cudf::data_type> key_cast_types;  // per-key cast so INT32/INT64 keys agree
};
```

A sink with more than one destination and no key columns is a construction error: silently
routing every row to destination 0 would corrupt a downstream shuffle rather than fail loudly.

Output stream id, partition index, and stream correspond **positionally**. Each destination has
its own `batch_stream`, all constructed with the same single expected sender (the pipeline), so
all partitions reach EOS together when `on_finalize_operator()` closes them — but `drained(i)`
and `wait(i)` are per-stream, so an undrained partition stays distinguishable from EOS
independently of its siblings.

N and the partition spec come from the StarRocks exchange descriptor via translation. *Which*
compute node each partition ships to is the wrapper's routing table — never the sink's. The sink
stays oblivious to destinations.

## `exec::stream_session` — the id-addressed router

**Files:** `src/include/exec/stream_session.hpp`, `src/exec/stream_session.cpp`

```
push(stream_id, batch)              // → source.push
close_input(stream_id, sender_id)   // → source.close_input(sender)
pull(stream_id) -> optional         // → sink.pull(partition)
wait(stream_id)                     // → sink.wait(partition)
drained(stream_id) -> bool          // → sink.drained(partition)
```

One session models **one plan fragment**, and stream ids are session-local. Ids are
**direction-separated**, two independent namespaces: `push`/`close_input` resolve *input*
streams (sources), `pull`/`wait`/`drained` resolve *output* streams (sink partitions). A
partitioned sink registers N ids, one per destination, so `pull(stream_id)` addresses exactly one
partition. An unknown id is a defined error.

The session holds **no repositories** — it forwards to the operators, which own the streams. It
does not own the operators either: a plan tree owns its children as `duckdb::unique_ptr`, so
whatever owns the plan must outlive the session. It builds no plan, submits nothing to the
scheduler, and owns no teardown. A leaf-fragment session registers only sink ids (no source); a
root-fragment session registers a source id plus sink ids. Nothing inside the engine pairs a
leaf's output id with a root's input id; that pairing is the wrapper's routing table, built from
the front end's plan and applied across sessions and nodes.

> **Gotcha for the plan-launcher work.** The sink is the pipeline **tail**, and it lands in
> `operators[0]` so that finishing the pipeline reaches it and fires end-of-stream. A plan launcher
> must key on that structure rather than on `is_source()`.

## `exec::streaming_fragment` — one fragment, built and run

**Files:** `src/include/exec/streaming_fragment.hpp`, `src/exec/streaming_fragment.cpp`, with
`src/include/exec/stream_bind_catalog.hpp` and `src/include/exec/stream_plan_bindings.hpp` for
the plan-side binding.

```
fragment_spec spec = { plan_source, inputs, outputs, partitioning };
streaming_fragment frag(context, spec);
frag.build(query_id);   // declare → plan → create operators → register with session
// push batches into frag.session() here if this fragment has inputs
frag.run();             // blocks until all pipelines finish
// pull from frag.session() until drained
```

A plan says "this read is a stream" through `sirius_stream_source(id)`, a table function whose
bind resolves names and types from the per-connection `stream_bind_catalog` — a stream has no
file to probe, so the schema is **declared rather than inferred**. The catalog is registered as
a `ClientContextState` because DuckDB's bind runs long before physical planning: bind resolves
the schema from it, and the plan generator re-reads it at plan time. The generator lowers that
read to a `STREAMING_SOURCE` wired to the declared repository and sender set; the function body
itself throws if ever executed.

`streaming_fragment` takes a `fragment_spec` (a logical-plan source, input specs keyed by
stream id, output ids, optional partitioning), declares its inputs in the catalog, lowers the
plan, roots it in a `STREAMING_SINK`, and registers both ends with its session. Its
repositories are created **outside** `data_repository_manager_`, so the query window's
mandatory cleanup cannot touch them and a sender's output survives its own fragment — that is
what makes a sequential relay between fragments possible.

The caller owns the query window: `build(query_id)` takes the id the caller's
`SiriusContext::StandaloneQueryScope` minted, and `build()` + `run()` must be bracketed in that
one scope. A lifecycle opened between them resets the task creator and scan manager `build()`
populated, so the fragment runs zero tasks and returns an empty output with no error. `run()`
blocks until the fragment's pipelines finish.

`sirius::ffi::Fragment` (`src/include/sirius_ffi.hpp`, `src/sirius_ffi.cpp`) is the cxx-FFI
wrapper over this substrate: a Rust caller declares inputs and outputs, builds a Substrait plan
against them (each input read through a `sirius_stream_<id>` view), relays each sender in, and
runs, with engine exceptions crossing the bridge as `Result`. No batch type crosses cxx —
`relay_from` moves batches entirely inside C++, and Arrow appears only at a result fragment.

## Worked example: distributed GROUP BY

The flagship case composes entirely from the pieces above — no new operator, no new mechanism.
StarRocks' front end emits two fragment shapes:

```
Leaf fragment (every CN, over its shard)      Root fragment (every CN, owns one key range)
  partitioned STREAMING_SINK                    STREAMING_SINK (N = 1)
  └─ HASH_GROUP_BY  (partial)                   └─ MERGE_GROUP_BY (final)
     └─ GPU_SCAN                                   └─ STREAMING_SOURCE
                                                      (expected = {0 … N-1})
```

The shuffle in the middle — what is `PARTITION` → port → `MERGE` on a single node — becomes the
sink's N per-destination streams, the wrapper's transport hop, and the source's sender-aware
fan-in. The **aggregate algebra is unchanged** (`SUM→SUM`, `COUNT→SUM` of partial counts, `AVG`
carrying `(sum, cnt)`); distributed GROUP BY is a data-movement and lifecycle problem, which is
exactly the seam these operators fill.

Two shapes fall out, and both are supported:

- A **leaf** session registers only a partitioned sink — *no source*. Its EOS comes from the scan
  finishing → `on_finalize_operator()`, not from any `close_input`. A session with no input
  streams is legitimate.
- A **root** session's single source is fed by N remote senders, and reaches EOS only after all N
  *distinct* senders close. A repeated close from one sender cannot terminate it early.

## Why no backpressure

Dropping channel-level backpressure is a considered bet, not an omission.

**Sirius has no way today to propagate a "stop producing" signal upward.** Task hints are only
`READY` / `WAITING` / nothing — there is no "slow down" variant. True backpressure (akin to
throttling scan prefetch) is distinct from the executor's memory self-backpressure, and needs
co-design with scheduling, prioritization, and query concurrency. Baking a guess into the
streaming layer now would almost certainly be re-done once concurrency lands.

What relieves pressure instead is the **downgrade executor**: queued batches sit in repositories
where the memory sweep can see and spill them (GPU → host → disk).

Supporting reasons:

- **Single-query is fine without it.** DuckDB plans are topologically sorted, so within one query
  there are few wasted tasks and dependencies are ordered.
- **The real pressure problem is cross-fragment / cross-query.** The intended lever there is
  **per-fragment priority** (extending today's per-operator priority), not a channel — so
  single-node scheduling principles carry into the distributed world.
- **Longer term**, a *minimal* sink↔source signal for remote slowness or skew can coexist with
  this design. Nothing here forecloses it: the hooks and future priority hooks are additive.

`batch_stream` never infers pressure from queue depth.

## Migration from `exec::exchange_channel`

The earlier design pushed lightweight batch handles through a bounded `exec::exchange_channel`
that the source resolved against a repository. `exchange_channel` conflated a **queue of data**
(which the repository already is) with **stream state**. It has been deleted.

| `exchange_channel` concept | Replaced by |
|---|---|
| Queue of batch handles | `shared_data_repository` (owner of record; spillable) |
| `close()` | `batch_stream::close(sender_id)` — per-sender, idempotent |
| `drained()` | `batch_stream::drained()` |
| open-empty vs closed | `batch_stream::classify()` |
| admission (was capacity) | `batch_stream::push()` — refused once terminal |
| producer failure | `batch_stream::fail()` — P1–P4, rethrow out of `try_pull()` |
| `on_push` re-arm | persistent `on_data` hook → `task_creator::schedule(head)` |
| `on_close` re-arm | `on_end_of_stream` hook → `update_pipeline_status(false)` |
| `on_pop` (backpressure resume) | **removed** |
| item / byte capacity bounds | **removed** |

## Not here yet

Scoped out deliberately; each is tracked separately.

- **Non-blocking `run()`.** `run()` blocks until the fragment's pipelines finish, and the query
  lifecycle slot in `SiriusContext` is global single-flight, so fragments cannot overlap.
  Per-query lifecycle isolation is the blocker; everything here is already written for it.
- **The source of the expected sender population.** The sender-aware API and its dedup ship now;
  where N comes from (StarRocks fragment metadata, surfaced by translation) is wrapper-side work.
- **Bit-exact StarRocks partition hashing** — FNV/XXH3 for ordinary `HASH_PARTITIONED`,
  CRC32/bucket-id for the bucket-shuffle regime. For a local, single-node cut any consistent hash
  co-locates equal keys, so Sirius's own hash is correct here; cross-node correctness needs the
  exact function and lands with translation.
- **Per-destination coalescing** (batching small slices before flush, to avoid a flood of tiny
  transfers) — owned by the transfer path.
- **Order-preserving / range partitioning** for merging exchanges. v1 is order-insensitive only
  (hash-join build, aggregation).
- **Node-to-node exchange** (pull → request destination → transfer → cleanup).
- **Generic consumer / Arrow conversion policy.** `pull()` returns native batches in their
  current tier; conversion is a consumer-side policy.

## Tests

| File | Catch2 tag |
|---|---|
| `test/cpp/exec/test_batch_stream.cpp` | `[batch_stream]` |
| `test/cpp/operator/test_physical_streaming_source.cpp` | `[streaming_source]` |
| `test/cpp/operator/test_physical_streaming_sink.cpp` | `[streaming_sink]` |
| `test/cpp/exec/test_stream_session.cpp` | `[stream_session]` |
| `test/cpp/exec/test_stream_bind_catalog.cpp` | `[stream_bind_catalog]` |
| `test/cpp/exec/test_streaming_fragment.cpp` | `[streaming_fragment]`, one control case under `[streaming_fragment_control]` |
| `test/cpp/pipeline/test_streaming_sink_root.cpp` | `[streaming_sink_root]`, one execution case under `[streaming_sink_root_exec]` |

`BSTR-*` cases cite the P1–P4 labels directly; a `recording_task_creator` stands in for the
scheduler, so the live re-arm is proven without a live executor.
