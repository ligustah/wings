# Undecided tx stranded by a deleted participant stream (2026-09-16)

Surfaced by a wings 4-node GCP-spot p2p chaos test (coordinator + 4 worker/broker
nodes over a tailnet). Versions: durable_streams v0.172.0, broker v0.277.0,
dsclient v0.58.0, dswire v0.41.0.

## What happens

1. A worker runs a job attempt. Its history is written transactionally:
   producer id `wings.job.<job>.<thread>.<attempt>`, participant stream
   `wings.history.<job>.<attempt>.<thread>`.
2. I hard-kill that worker mid-attempt. Its transaction is left **prepared /
   undecided** on the coordinating broker.
3. wings redispatches the job to another worker; the job eventually settles from a
   later attempt. On settle, wings' normal cleanup **deletes the abandoned
   attempt's history stream** (`dropOutputsOf` → `dropStream`).
4. The coordinator broker then logs this every ~5s, apparently forever (well past
   the 5-minute tx budget), and the whole run wedges — the workflow's fan-out
   never completes:

```
level=WARN msg="could not finish a transaction left undecided"
  tx=wings.job.u10551-h.main_42.1 state=PREPARE_ABORT
  err="no resolver claims this participant and this process does not hold its log:
       wings.history.u10551-h.1.main_42/0;
       participants: wings.history.u10551-h.1.main_42/0=unresolved
       (stream deleted at 2026-09-15T22:03:46.89Z)"
```

Two jobs (u10551-h, u10551-r) wedged this way. Redispatch of other jobs worked,
and the dead broker was fenced with `sole_copies=0` (no data loss) — so this is
specifically about resolving the killed attempt's prepared tx after its
participant history stream was deleted.

## Questions for DS

1. When a **participant stream of an undecided (prepared) transaction is
   deleted**, is it expected to become permanently unresolvable like this? Should
   DS treat a deleted participant as an implicit abort, or refuse deleting a
   stream that still participates in an undecided tx?
2. Does one stranded undecided tx **block its tx-state partition's committed
   boundary / other transactions** on that coordinator? Trying to tell whether
   this is just log noise or the actual cause of the run wedging (other jobs'
   commits also appeared stalled).
3. Fix ownership: should DS resolve/abort a tx whose participant was deleted, or
   must **wings abort a dead worker's outstanding attempt transactions before
   dropping their streams**? If wings-side, is there an API to abort a prepared tx
   by an identity whose original producer (the killed worker) is gone?

## wings-side detail

`dropOutputsOf` (output.go) runs on job settle and deletes every abandoned
attempt's history stream. It has no knowledge of whether that attempt's tx is
still undecided — for a killed worker it always is, since the worker never
committed or aborted before dying.

## Local reproduction result

A local `LocalProcess` p2p worker-kill (a checkpointing job in flight, worker
hard-killed mid-attempt) does NOT reproduce the wedge — the run completes and the
work redispatches. See `TestP2PWorkSurvivesAWorkerKilledMidCheckpoint`. So the
wedge is timing / overlay-latency dependent: on GCP the killed node was also a
broker holding replicas, fenced over a ~2-minute grace under overlay latency,
which widened the window between the prepared tx being stranded and its participant
history stream being dropped. Reproducing it locally likely needs a longer commit
interval and/or a delayed reap, or a slowed peer link — a lead to pursue once DS
confirms whether the deleted-participant tx is meant to be resolvable.
