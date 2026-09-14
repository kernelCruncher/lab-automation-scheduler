# Notes

**Time spent:** 3.5 hours

**Design.** Every event — a run starting, a driver reporting — ends in the same
`_pump(run_id)`, which asks the database what is runnable and offers it to the
drivers. No plan is held in memory; the `steps` table is the only state. A
per-run `asyncio.Lock` serialises `_pump`, and `Store.claim_step` is an atomic
`UPDATE ... WHERE status='pending' RETURNING id`. I added two methods to `Store`
(`claim_step`, `release_step`) because the claim cannot be expressed with the
shipped ones; nothing else outside `scheduler.py` changed.

## 1. What I deliberately did not build

- **Recovery from lost results.** The two bus directions differ. Commands are
  request/reply: within 3s I get accepted, refused, or a `BusError`, so I always
  learn the outcome and a refusal is an answer rather than a lost message.
  Results are fire-and-forget with no persistence, so a dropped result, an
  executor restart and a driver crash are indistinguishable: silence. There is
  no timeout anywhere in the executor. Handling it needs a deadline sweep over
  `dispatched` steps plus a per-instrument timeout that is a lab decision, not
  mine. The brief puts this out of scope.
- **Crash recovery.** In-memory state dies with the container and nothing scans
  for orphans at startup, so a restart strands in-flight runs at `running`.
- **A device reservation queue.** Contention is still resolved by asking and
  being refused rather than by booking an instrument ahead of time, so there is
  no admission control and a burst of runs wastes commands on refusals. Bounded
  waiting did not need it (see the third finding below), and a reservation would
  duplicate state the instruments already own authoritatively.

## 2. Where it is most likely to break

- **A raised exception stalls the run silently.** `bus.py` catches anything the
  result handler throws, logs it and moves on, so a bug in `handle_result` does
  not crash anything — the run just stops, looking exactly like a hang. This is
  the most likely failure mode of my own code.
- **`handle_result` sweeps every running run.** Necessary for both liveness and
  fairness (see the findings below) but it costs: `list_runs` returns every run
  ever created and I sort that whole list on each result, the pumps are
  sequential so one unreachable driver's 3s timeout delays every other run, and
  a run orphaned by a restart gets silently adopted by the next result that
  arrives. The first wants an indexed `status = 'running'` query.
- **`BusError` with nothing in flight.** If a driver is unreachable I release the
  step and log; if that run has nothing else running, nothing will ever pump it
  again. Same missing machinery as the dropped-result case.

## 3. If two drivers report a step finished at the same moment

This happens on ordinary runs, not only in the test. On one run
`fill_buffer_plate` and `warm_reagent_plate` both reported at 12:16:29.502 —
together the last two of `combine`'s three predecessors.

What happens:

1. The bus gives each result its own task, so both enter `handle_result` at
   once.
2. Each records its own step. Different rows, so these do not contend.
3. Both then call `_pump` for the same run, and the per-run lock admits one.
4. The winner re-reads the steps table, sees _both_ results already recorded,
   finds `combine` unblocked, claims it and sends it.
5. The loser re-reads, finds `combine` already `dispatched`, and sends nothing.

Being second costs nothing. Because a pump re-derives everything from the table
instead of acting on the result it was handed, the winner acts on both results,
not just its own — and for the same reason the order they arrive in is
irrelevant.

`claim_step` is a second, independent guard. Without the lock, both pumps could
still conclude `combine` was runnable; the conditional UPDATE means only one
finds a row still in `pending`, and only that one sends. In a single process
that is redundant. It is what would hold if the executor were ever replicated,
where an `asyncio.Lock` means nothing.

The part worth flagging is the refusal, not the claim. The lock is held across
`send_command`, not just around the claim, because a refused step is put back to
`pending`. A pump running in the window between the refusal and the reset would
see the step as `dispatched` and skip it — and with nothing of its own in
flight, the run would stall with nothing left to wake it.

## The `bus.py` concurrency line

Not one of the three questions above — this answers the aside in "The driver
contract": _"it is one line in `bus.py` and you are free to change it. Either
way, say which you picked and why."_

Kept as shipped — one task per result.

Changing it to `await handler(result)` would make the NATS callback wait for
each handler to finish before it reads the next message: a database write, a
re-read of every running run, and up to 3s of that if a driver has gone quiet.
Every result would queue behind that one, including results for unrelated runs.
Nothing here needs that. The per-run lock already allows only one decision at a
time for the run being decided, and that is the only place two results can
collide.

What it costs: this line is what makes the race in question 3 possible at all,
and an exception in a handler is logged and swallowed rather than propagated.

## Where I went further

**Concurrent runs.** The brief never says whether these are in scope and none
of the four checks exercise them, so I tried two at once. They deadlocked: B's
first step was refused because A held the liquid handler, B returned it to
`pending`, and nothing pumped B again — only B's own results pumped B, and B
had nothing in flight. Making `handle_result` sweep every running run fixed it,
and then created a fairness bug I would not have predicted. Two biases, both
against age: the run that just reported got first refusal on the device it had
itself freed, and the rest were swept newest-first, because that is the order
`list_runs` returns. With a new run arriving every 2.5s, a waiting run was
refused 67 times in 75s and completed **none** of its seven steps while newer
runs finished. Sweeping oldest-first with no priority for the reporting run
bounds the wait: a run can only be delayed by runs older than itself, and there
is a fixed number of those. The same test now finishes the victim in 18s, which
is exactly that bound — it waits for the one run ahead of it.

`tests/test_concurrent_runs.py` is the one test I added, covering the deadlock:
two runs against a bus that refuses while busy, both must finish. I checked it
earns its keep by reverting the sweep — it fails naming the stalled run, while
the supplied test still passes. It also asserts a refusal actually happened, so
it cannot pass vacuously if that path stops being reached. The starvation half
is not covered; it depends on arrival timing and I did not want a flaky test.

**Critical-path ordering.** `list_steps` returns steps by name, so
`fill_buffer_plate` beat `fill_reagent_plate` to the liquid handler at t=2.
Reagent is on the critical path, so the incubator idled and the run took
12.16s. `_downstream_depth` orders ready steps by the work stacked behind them
instead: 10s, the optimum for this shape.

---

## Feedback on the exercise

**Was it clear?** Very, on the four rungs, and an interesting challenge. One
real gap: it never says whether concurrent runs are in scope. I assumed they
were, and testing that assumption is what turned up the two failure modes
described above — both of which the submitted scheduler handles.

**How long.** 3.5 hours, against the suggested two.

**Worth the time.** The central question — where exclusivity lives: the
driver's busy flag, an in-process lock, or a database claim — has more than one
defensible answer, and it is what I would want to be asked about.
The supplied concurrency test earns its place too; forcing two results into the
same instant with a barrier catches a class of bug that real instruments hide.
Nothing was wasted except the rebuild cycle below.

**What got in the way, and what I would change.**

- What makes this about instruments rather than job queues is that drivers
  refuse rather than queue — and nothing reliably verifies that path. The
  supplied test's bus accepts every command, and the three shell checks each
  run one workflow at a time, so whether a refusal happens at all depends on
  the implementation: mine skips devices the run already holds, and
  `acceptance.sh` duly reports zero. A fifth rung — _two runs at once both
  finish_ — would make rung 3 unavoidable.
- No way to abort a run through the API, so clearing stuck runs meant `psql`.
  `RUN_ABORTED` and `STEP_RUNNING` are defined and never used; as shipped they
  read like hints about a design that is never described.
