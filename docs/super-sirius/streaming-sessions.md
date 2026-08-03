# Streaming Sessions

How data enters and leaves a Sirius plan fragment when the fragment is one hop of a larger,
distributed query. Four pieces: a **streaming source** (`STREAMING_SOURCE`), a **streaming
sink** (`STREAMING_SINK`), the `exec::batch_stream` primitive both are built on, and the
`exec::stream_session` router that addresses them by stream id.

Sirius itself stays fragment-blind. It never learns that it is distributed, which compute node a
partition ships to, or how many nodes exist. All of that lives in the wrapper above the engine.

## The model: the repository is the queue

A `cucascade::shared_data_repository` already *is* a thread-safe queue of `data_batch`es, and it
is the owner of record the downgrade executor sweeps for spill candidates. What it lacks is the
**lifecycle** of a stream: who is still producing, whether "nothing right now" means *wait* or
*the stream is over*, and how a starved consumer gets woken.

So each streaming operator owns two things:

| Concern | Owned by |
|---|---|
| The queue of batches | `cucascade::shared_data_repository` |
| End-of-stream, availability, waking | `exec::batch_stream` |

Producers push into the repository and consumers pull from it, directly. There is **no
bounded channel and no channel-level backpressure** — see [Why no backpressure](#why-no-backpressure).

Batches cross this boundary **natively**, as `cucascade::data_batch`, in whatever tier they
currently sit. Nothing is materialized to Arrow on the way in or out, so a queued batch stays
spillable (GPU → host → disk) right up until it is pulled, and `pull()` hands it back in its
current tier without forcing an upgrade.

## `exec::batch_stream`

**Files:** `src/include/exec/batch_stream.hpp`, `src/exec/batch_stream.cpp`

One direction of batch flow: N declared senders push `data_batch`es into one
`shared_data_repository`; consumers pull, poll, or block. The repository is the queue;
`batch_stream` owns everything the repository lacks — who is still producing, whether "nothing
right now" means *wait* or *over*, how a starved consumer gets woken, and how a producer failure
reaches the consumer.

```cpp
class batch_stream {
 public:
  enum class availability { HAS_DATA, WAITING, END_OF_STREAM };

  batch_stream(shared_ptr<shared_data_repository> repo, set<sender_id_t> expected);

  // Producer
  [[nodiscard]] bool push(shared_ptr<data_batch>);   // S1: in repo before on_data fires; false once terminal
  void close(sender_id_t);       // idempotent per sender; set-based fan-in
  void fail(exception_ptr);      // P1–P4: immediate, first-wins, fail-fast, announces like data

  // Consumer
  shared_ptr<data_batch> try_pull();  // S4: rethrows pending error before pop
  availability classify() const;      // S3: errored stream is never END_OF_STREAM
  bool drained() const;               // clean end only (S3)
  void wait();                        // S5: not atomic with try_pull — loop must re-check

  // Hooks (single slot; fire after unlocking; late registration on an ended stream fires immediately)
  void set_on_data(function<void()>);           // persistent — fires on every push and on fail()
  void set_on_end_of_stream(function<void()>);
};
```

Three things are load-bearing.

**End-of-stream is a set, not a counter.** A fan-in stream — one root source fed by N remote
leaves — is over only when *all N* senders have closed. A counter cannot tell "both senders
closed once" from "one sender closed twice". `close(sender_id)` inserts into a set and compares
against the expected set; a repeat close is a no-op, an unexpected id is a defined error.

**Push, close, and every emptiness check share one lock (S1).** `push()` inserts the batch
*under* the stream's lock and fires `on_data` after releasing it, so a close cannot interleave:
no batch is ever admitted after end-of-stream, and every batch is in the repository before the
wake that announces it. Callbacks fire after unlocking, so a hook may re-enter the scheduler.

**`classify()` separates "not yet" from "never" (S3).** Queued data outranks terminal: EOS is
never reported while a batch the stream already accepted is still pullable. A pending error reads
as `HAS_DATA` even over an empty queue — the only way out is the rethrow from `try_pull()`, not
a clean finish that would let a failed query succeed silently.

| `_terminal` | `_error` | repo empty? | `classify()` |
|---|---|---|---|
| no | no | no | `HAS_DATA` |
| no | no | yes | `WAITING` |
| yes | no | no | `HAS_DATA` |
| yes | no | yes | `END_OF_STREAM` |
| either | yes | either | `HAS_DATA` (P4) |

`wait()` blocks until `classify() != WAITING`. Engine workers never call it (S5).

## `STREAMING_SOURCE` — the input boundary

**Files:** `src/include/op/sirius_physical_streaming_source.hpp`, `src/op/sirius_physical_streaming_source.cpp`

Wraps one `exec::batch_stream` constructed with the fragment's expected sender set. Producers call
`push(batch)` and `close_input(sender_id)`; the engine sees an ordinary source.

| Stream state | `get_next_task_hint()` |
|---|---|
| `HAS_DATA` | `READY{this}` |
| `WAITING` | `WAITING{nullptr}` |
| `END_OF_STREAM` | `std::nullopt` |

`all_ports_empty()` is `stream.drained()` (clean end only — an errored stream stays `false`);
`get_next_task_input_data()` calls `stream.try_pull()` (one batch per task, zero-copy, rethrows
on pending error); `execute()` is a pass-through.

### The live re-arm

The engine is pull-scheduled and event-poor. A head that answers `WAITING{nullptr}` is dropped,
and the only built-in re-nomination is task completion — so a stream-fed source that starved
(open and empty) has **no completing task to wake it**. Without a live re-arm the streaming
source would only ever run when some other task happened to be in flight.

`batch_stream::set_on_data` is a **persistent** (not one-shot) hook wired in `set_pipeline()`.
Every successful `push()` fires it, calling `task_creator::schedule(head)` — which only enqueues
onto the thread-safe creation queue, so it is safe to fire from any thread (a GPU worker
mid-`sink()`, the wrapper's network thread). The callback weak-captures the pipeline, never
`this`, and resolves the head through `pipeline->get_source()`. A late schedule after
`task_scheduler::drain_after_error()` is dropped by the interrupted-queue path.

Because the hook is persistent, a `push()` can never race past a lost notification: there is no
waker to re-arm, so there is nothing to miss.

Separately, `set_on_end_of_stream` → `pipeline->update_pipeline_status(false)` handles the case
the `on_data` hook cannot: a stream that closes with **no task in flight** (an empty stream, or a
late close after the last task completed) has nothing to call `update_pipeline_status()` for it.
`false` (rather than the default `true`) matters — it makes `notify_downstream_pipelines()` also
schedule this pipeline's consumers. Registering the hook after the stream already ended fires it
immediately, so a raced close is not lost.

## `STREAMING_SINK` — the output boundary

**Files:** `src/include/op/sirius_physical_streaming_sink.hpp`, `src/op/sirius_physical_streaming_sink.cpp`

A pipeline-terminal operator. `sink()` pushes each output batch into an output `batch_stream`
via `push()`; `on_finalize_operator()` — the existing pipeline-finish hook — calls
`stream.close(PIPELINE_SENDER)`, which is what makes `END_OF_STREAM` observable.
Consumers use `pull(i)` / `wait(i)` / `drained(i)`, plus `availability(i)` for the non-blocking
three-way classification.

It is deliberately minimal: it overrides `sink()`, `on_finalize_operator()`, and the pass-through
`no_history_peak_memory_estimate`, and nothing else. It carries no parking buffer and no
closing state machine — those existed only to absorb a full bounded output channel, and there is
no channel. Unlike the source it registers **no `on_data` hook**: its consumer is an external
thread in `wait()`, not an engine task.

### Partition fan-out

A sink can expose **N output streams**, one per destination, each backed by its own repository.
`sink()` GPU-hash-partitions each batch by the `partition_spec`'s key columns (reusing
`gpu_partition_impl::hash_partition`, the same kernel the `PARTITION` operator uses) and pushes
slice *i* into repository *i*; empty slices are skipped rather than published as zero-row
batches a consumer would pull and discard. A slow receiver's backlog accumulates in its own
repository — spillable by the downgrade executor — without head-of-line-blocking the others.
The single-destination sink is simply the N = 1 case, and keeps the identity-preserving
no-partition path.

```cpp
struct partition_spec {
  std::vector<int> key_columns;                 // hashed to pick a destination
  std::vector<cudf::data_type> key_cast_types;  // per-key cast so INT32/INT64 keys agree
};
```

A sink with more than one destination and no key columns is a construction error: silently
routing every row to destination 0 would corrupt a downstream shuffle rather than fail loudly.

Output stream id, partition index, and repository correspond **positionally**. Each partition *i*
has its own `batch_stream(_outputs[i])`, so `drained(i)` and `wait(i)` are independent — a slow
receiver stays distinguishable from EOS even after its siblings drain. All partitions share the
same sender (`PIPELINE_SENDER = 0`), so `on_finalize_operator()` calling `close()` on every
stream drives all N to EOS together.

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

The session holds **no repositories** — it forwards to the operators, which own the queues. It
builds no plan, submits nothing to the scheduler, and owns no teardown. A leaf-fragment session
registers only sink ids (no source); a root-fragment session registers a source id plus sink ids.
Nothing inside the engine pairs a leaf's output id with a root's input id; that pairing is the
wrapper's routing table, built from the front end's plan and applied across sessions and nodes.

> **Gotcha for the plan-launcher work.** The sink is the pipeline **tail**, and it lands in
> `operators[0]` so that finishing the pipeline reaches it and fires end-of-stream. A plan launcher
> must key on that structure rather than on `is_source()`.

## Worked example: distributed GROUP BY

The flagship case composes entirely from the four pieces above — no fifth operator, no new
mechanism. StarRocks' front end emits two fragment shapes:

```
Leaf fragment (every CN, over its shard)      Root fragment (every CN, owns one key range)
  partitioned STREAMING_SINK                    STREAMING_SINK (N = 1)
  └─ HASH_GROUP_BY  (partial)                   └─ MERGE_GROUP_BY (final)
     └─ GPU_SCAN                                   └─ STREAMING_SOURCE
                                                      (expected = {0 … N-1})
```

The shuffle in the middle — what is `PARTITION` → port → `MERGE` on a single node — becomes the
sink's N per-destination repositories, the wrapper's transport hop, and the source's sender-aware
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
  this design. Nothing here forecloses it: the waker and future priority hooks are additive.

`batch_stream` never infers pressure from queue depth.

## Migration from `exec::exchange_channel`

The earlier design pushed lightweight batch handles through a bounded `exec::exchange_channel`
that the source resolved against a repository. `exchange_channel` conflated a **queue of data**
(which the repository already is) with **stream lifecycle**. It has been deleted.

| `exchange_channel` concept | Replaced by |
|---|---|
| Queue of batch handles | `shared_data_repository` (owner of record; spillable) |
| `close()` | `batch_stream::close(sender_id)` — per-sender, idempotent, set-based |
| `drained()` | `batch_stream::drained()` |
| open-empty vs closed | `batch_stream::classify()` |
| admission (was capacity) | `batch_stream::push()` — returns false once terminal (S1) |
| `on_push` re-arm | `batch_stream::set_on_data` persistent hook → `task_creator::schedule(head)` |
| `on_close` re-arm | `batch_stream::set_on_end_of_stream` hook → `update_pipeline_status(false)` |
| `on_pop` (backpressure resume) | **removed** |
| item / byte capacity bounds | **removed** |

## `exec::streaming_fragment` — plan builder + blocking runner

**Files:** `src/include/exec/streaming_fragment.hpp`, `src/exec/streaming_fragment.cpp`

Owns a complete fragment life cycle: declares inputs, builds the plan, constructs the sink, runs,
and keeps the output pullable after `run()` returns.

```
fragment_spec spec = { plan_source, inputs, outputs, partitioning };
streaming_fragment frag(context, spec);
frag.build(query_id);   // declare → plan → create operators → register with session
// push batches into frag.session() here if this fragment has inputs
frag.run();             // blocks until all pipelines finish
// pull from frag.session() until drained
```

Two lifetime decisions are load-bearing:

**Repositories outlive the engine.** The fragment creates every repository before planning and
registers none of them with `data_repository_manager_`. The query window's mandatory cleanup
(`StandaloneQueryScope::finish()`) therefore cannot touch them. A sender's output stays in its
repository and is still there when the receiver runs — which is what makes sequential streaming
work without copying.

**One query window, shared.** `run()` reuses the caller's `StandaloneQueryScope` rather than
opening its own. A second scope resets the task creator and scan manager that `build()` populated;
the fragment would then run zero tasks and return silently empty. The caller brackets `build()`
and `run()` in one window (as `Context::execute_substrait` does for ordinary queries).

**`stream_bind_catalog` bridges bind time and plan time.** DuckDB's table-function bind runs
long before physical planning. The catalog is registered as a `ClientContextState` so
`sirius_stream_source(id)` can resolve a schema at bind time; the physical plan generator
re-reads the catalog at plan time to build each `STREAMING_SOURCE`.

## Not here yet

Scoped out deliberately; each is tracked separately.

- **Non-blocking execution and task teardown.** `streaming_fragment::run()` blocks until all
  pipelines finish (`engine->execute()` is synchronous). Non-blocking submission to
  `task_scheduler` and safe teardown with tasks in flight come later.
- **The source of the expected sender population.** The sender-aware API and its dedup ship now;
  where N comes from (StarRocks fragment metadata, surfaced by translation) is later work.
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
| `test/cpp/exec/test_streaming_fragment.cpp` | `[streaming_fragment]` |

A `recording_task_creator` stands in for the scheduler, so the live re-arm and the `on_data`
hook path are proven without a live executor. `test_streaming_fragment.cpp` requires a GPU and
a real DuckDB integration database (`[integration]` tag).
