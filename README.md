# Lab Automation — Workflow Executor

## Context

This is a slice of a lab automation platform: software that drives robotic lab
equipment through scientific workflows.

- **Drivers** stand in for physical instruments — a liquid handler, an incubator,
  a plate reader. Each does **one thing at a time**, and will refuse a command
  while it is busy. Real instruments do not politely queue.
- **A workflow** is a DAG of steps. Each step names the device that must run it
  and the steps that have to finish before it can start.
- **The executor** owns all run and step state, and decides what runs when.

Workflows live in `config/workflows.yaml`. The default one:

```
fill_sample_plate ──┬──> incubate_samples ──────────────────────────┐
                    │                                               │
                    ├──> fill_reagent_plate ──> warm_reagent_plate ──┤──> combine ──> read_plate
                    │                                               │
                    └──> fill_buffer_plate ─────────────────────────┘
```

| Step | Device |
| --- | --- |
| `fill_sample_plate` | liquid handler |
| `incubate_samples` | incubator |
| `fill_reagent_plate` | liquid handler |
| `fill_buffer_plate` | liquid handler |
| `warm_reagent_plate` | incubator |
| `combine` | liquid handler |
| `read_plate` | plate reader |

Three things about this shape:

- **`combine` genuinely needs three inputs.** It is a liquid handling step that
  draws from the reagent plate and the buffer plate into the incubated sample
  plate. It cannot start until all three are ready — the pipette would have
  nowhere to draw from.
- **`fill_reagent_plate` and `fill_buffer_plate` have no dependency between them
  and both become ready at the same moment, but both need the liquid handler.**
  It can only take one of them.
- **Work on different devices can overlap.** `incubate_samples` and the liquid
  handler steps are independent of each other.

That file is read fresh on every run, so you can edit it and start another run
without restarting anything. Add your own shapes if it helps you.

## Architecture

```
                      ┌──────────────┐
                      │   executor   │  owns run + step state
                      │  (port 5001) │  decides what runs when   <-- your work
                      └──┬────────┬──┘
                         │        │
              bus: command / result / state
                   │         │         │
           ┌───────▼──┐ ┌────▼─────┐ ┌─▼──────────┐
           │ liquid-  │ │incubator-│ │ plate-     │
           │ handler-1│ │    1     │ │ reader-1   │
           └──────────┘ └──────────┘ └────────────┘
                         │
                    Postgres (executor only)
```

## What is here, and what is not

**Provided and working — you should not need to change these:**

| | |
| --- | --- |
| `services/worker/` | The three drivers. One step at a time, refuse when busy, report their own state |
| `services/executor/bus.py` | The bus: send a command, receive results, query a driver |
| `services/executor/store.py` | Postgres access for runs and steps |
| `services/executor/api.py` | The HTTP API, including a timeline view |
| `config/workflows.yaml` + loader | Definitions, parsed and validated |
| `scripts/acceptance.sh` | Checks whether your executor does its job |

**Not implemented — this is the exercise:**

`services/executor/scheduler.py`

Everything is yours to change if you want to, including the database schema in
`db/init.sql`. Nothing outside `scheduler.py` has to stay as it is.

Two exceptions, because the checks call into them: keep the signatures of
`Scheduler(store, bus)`, `Scheduler.start` and `Scheduler.handle_result`, and
keep `Store(pool)`. Everything inside them is yours.

## The driver contract

Sending a step is a **request** and the driver answers immediately:

```python
ack = await bus.send_command(StepCommand(...))
# ack.accepted is False when the driver is busy. This is not an error.
```

`send_command` returns as soon as the driver has accepted or refused. The step
**finishing** arrives separately, later, through the handler you register with
`on_step_result`.

**Those handlers are called concurrently** — one asyncio task per result, so a slow
handler cannot hold up the bus. That was our choice, not a law of nature: it is one
line in `bus.py` and you are free to change it. Either way, say which you picked
and why.

`bus.driver_state(device_id)` asks a driver what it is doing. You do not need
it to build a working executor — it is there if you want it.

## Getting started

Prerequisites: Docker and Docker Compose, `curl`, `jq`.

```bash
docker compose up --build
```

First startup takes about a minute while Postgres seeds. After changing Python code:

```bash
docker compose up --build executor
```

Then:

```bash
curl -s localhost:5001/health    | jq
curl -s localhost:5001/workflows | jq
curl -s localhost:5001/drivers   | jq     # what each instrument has done

./scripts/acceptance.sh                   # will fail until you build the scheduler
```

### Making instruments misbehave

Each driver takes two knobs, both `0` by default. Set them in
`docker-compose.yml`, or from your shell without editing anything:

| Driver | Fail | Drop |
| --- | --- | --- |
| `liquid-handler-1` | `LH_FAIL_PCT` | `LH_DROP_PCT` |
| `incubator-1` | `INC_FAIL_PCT` | `INC_DROP_PCT` |
| `plate-reader-1` | `PR_FAIL_PCT` | `PR_DROP_PCT` |

Both take a whole number from 0 to 100. `/drivers` reports what each instrument
is set to, as `fail_pct` and `drop_result_pct`.

```bash
INC_FAIL_PCT=100 docker compose up -d incubator-1    # every step errors
INC_FAIL_PCT=0   docker compose up -d incubator-1    # back to normal
```

**Failing (`*_FAIL_PCT`) — the step goes wrong and the instrument says so.**
The result comes back with an error set. **Handling this is part of the
exercise**: the run should end as `failed`, not hang.
`./scripts/check-failure.sh` tests exactly this, and puts the driver back to
normal when it finishes.

**Dropping (`*_DROP_PCT`) — the instrument does the work and never reports
back.** The step really ran; the message is simply gone. **This one is not
required and we are not asking you to solve it.** It is there if you want to see
how the system behaves when a report goes missing, and it may be worth a
sentence in your note.

Set both back to `0` before running the acceptance checks — and note that
`check-failure.sh` will complain if it finds dropping switched on, because the two
interact.

---

# The exercise

**Two hours is a good amount of time.** Spend as much as you want, though — if
you want to keep going, feel free. Just tell us roughly how long you actually
spent. We are not scoring speed, and knowing the time is what lets us read
everything else fairly.

There are two tasks, and both need to be finished. Task 2 is a written note and
it is **required** — a submission without a `NOTES.md` is incomplete, and we
cannot read the code fairly without it. The note carries as much weight as the
code, so plan for it rather than fitting it in at the end: stop building while
you still have time to write it.

You are welcome to use AI assistance. We will ask you to walk us through your
reasoning and your choices, so work in a way that lets you do that.
If you can share your claude/codex coversations that would be ideal.

## Task 1 — build the scheduler

Implement `services/executor/scheduler.py` so that starting a run executes the
workflow. Build it in this order — each rung is worth having before you start
the next.

1. **Execute the DAG.** Every step runs exactly once, `depends_on` respected,
   and the run reaches `completed`.
2. **Overlap what can overlap.** Independent steps on different devices run at
   the same time. A run that could take 10 seconds should not take 14.
3. **Cope with refusal.** A busy driver refuses a command. That is normal, not
   an error — but a step that is refused and then forgotten stalls the run.
4. **End a bad run.** A step that reports an error ends the run as `failed`,
   rather than hanging it.

Those four are the exercise, and they are what we check. Going further is your
call — if you do, or if you decide not to, the note is where you tell us. Stop
with time left to write it.

Check your work:

```bash
./scripts/acceptance.sh                   # five checks against a live run
./scripts/acceptance.sh "Triple Assay"    # a second shape
./scripts/check-failure.sh                # a failing step must end the run
docker compose run --rm tests             # pytest

./scripts/timeline.sh <run_id>            # see what actually overlapped
```

The five acceptance checks are the main thing. `check-failure.sh` makes one
instrument report an error and expects the run to end as `failed` rather than hang.
The concurrency test forces two steps to finish at the same instant, which real
instruments will not reliably do for you.

If you cannot get everything green, say what is left in your note rather than
spending extra time.

## Task 2 — the note (required)

**Do not skip this, and do not leave it to the last five minutes.** We read the
note beside the code and **we spend a good part of the interview on it**. Write
it even if Task 1 is unfinished — especially then, because the note is where you
tell us what is missing and why.

Add a `NOTES.md` and commit it with your code. Start with roughly how long you
spent, then around 300–400 words answering:

1. What did you deliberately **not** build, and why?
2. Where is your implementation most likely to break, and what would you do
   about it?
3. If two drivers report a step finished at the same moment, what happens in
   your code?

Bullets are fine. No diagrams needed, and please do not spend time on
formatting — we are reading it for the reasoning. If there is something else you
think we should know, add it at the end.

### Then tell us what you thought of the exercise

Finish the note with a short section — a few bullets is plenty — about the
technical challenge itself:

- Was the brief clear? What did you have to guess at?
- How long did it really take, against the two hours we suggested?
- What was worth your time, and what was not?
- Did anything get in the way — setup, tooling, the checks, unclear scope?
- What would you change?

We are genuinely interested in what you thought, so be blunt. We read this part
like the rest of the note: a clear critique of the exercise tells us how you
think. Criticising it will not count against you.

## Submitting

Commit your work, push it to a repository of your own on GitHub, and send us the
link.

Before you send it, check that:

- `NOTES.md` is committed — without it the submission is incomplete
- it says roughly how long you spent
- it answers the three questions
- it ends with your feedback on the exercise

---

## API reference

### executor (port 5001)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET  | `/health` | Health check |
| GET  | `/devices` | Devices known to the executor |
| GET  | `/drivers` | Live state of each driver, and what it has executed |
| GET  | `/workflows` | Workflows defined in `config/workflows.yaml` |
| GET  | `/runs` | List runs |
| POST | `/runs` | Create a run. Body optional: `{"workflow_name": "..."}` |
| GET  | `/runs/{id}` | Run plus all its steps |
| POST | `/runs/{id}/start` | Start a run |
| GET  | `/runs/{id}/timeline` | When each step occupied its device |

```bash
run_id=$(curl -sf -X POST localhost:5001/runs -H 'Content-Type: application/json' -d '{}' | jq -r .id)
curl -sf -X POST "localhost:5001/runs/$run_id/start"
./scripts/timeline.sh "$run_id"
```

## Questions

Ask at any point.
