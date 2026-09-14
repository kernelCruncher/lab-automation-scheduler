import asyncio
import logging

from .bus import Bus, BusError, StepCommand, StepResult
from .models import (
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_RUNNING,
    STEP_COMPLETED,
    STEP_DISPATCHED,
    STEP_FAILED,
    STEP_PENDING,
    Step,
)
from .store import RunNotFound, Store

log = logging.getLogger(__name__)


class Scheduler:
    """Decides what runs when.

    The design in one line: every event -- a run starting, a driver finishing --
    ends in the same `_pump` call, which asks the database what is runnable now
    and offers it to the drivers. There is no plan held in memory; the steps
    table is the only state.

    Concurrency is handled twice over, deliberately:

      - A per-run asyncio.Lock serialises _pump. This is what makes the
        implementation correct today: there is exactly one executor process
        (uvicorn with no `workers=`), so one lock really does exclude every
        other decision-maker for that run.
      - Store.claim_step is an atomic conditional UPDATE, so a step can only be
        won once even if the lock were not there. This is redundant in the
        current deployment and cheap to keep; it is what would still be correct
        if the executor were ever replicated, where an in-process lock is worth
        nothing.

    The lock is held across send_command as well as the claim. That is the part
    that matters: a refused step is put back to 'pending', and if another task
    could pump in the window between the refusal and the reset, it would see
    the step as still 'dispatched', skip it, and the run would stall with
    nothing left to wake it.
    """

    def __init__(self, store: Store, bus: Bus) -> None:
        self.store = store
        self.bus = bus
        # One lock per run, so unrelated runs never wait on each other.
        # Created lazily; no await between the get and the set, so the event
        # loop cannot interleave and hand two tasks different locks.
        self._locks: dict[str, asyncio.Lock] = {}

    async def start(self, run_id: str) -> None:
        """Begin executing a run."""
        await self.store.start_run(run_id)
        await self._pump(run_id)

    async def handle_result(self, result: StepResult) -> None:
        """Record that a driver finished a step and move the run on.

        May be called concurrently.
        """
        if result.error:
            log.warning("%s failed on %s: %s", result.step_name, result.device_id, result.error)
        else:
            log.info("%s finished on %s", result.step_name, result.device_id)

        # Unknown step ids update no rows: the bus carries every driver's
        # reports, including for runs this executor never started.
        status = STEP_FAILED if result.error else STEP_COMPLETED
        await self.store.record_step_finished(result.step_id, status, result.error)

        await self._pump_all()

    async def _pump_all(self) -> None:
        """Settle every run still going, oldest first.

        An instrument has just come free, and the run waiting on it is not
        necessarily the one that freed it: a step refused because a *different*
        run held the device has nothing of its own in flight to wake it later.
        Without this sweep, every concurrent run but one stalls permanently.

        Oldest first, and with no priority for the run that just reported. Both
        matter. Giving the reporting run first refusal on the device it just
        freed is self-reinforcing -- the liquid handler is wanted by four of
        the seven steps in the default workflow, so the incumbent almost always
        has another step ready for it. Ordering the rest newest-first (which is
        what list_runs returns, for the API's benefit) then lets every new
        arrival jump the queue. Measured with both of those in place, a run
        waiting behind a new run every 2.5s was refused 67 times in 75s and
        completed none of its seven steps.

        Oldest-first with no incumbency means a run can only be delayed by runs
        older than itself, and there is a fixed number of those. That bound is
        what makes waiting finite.
        """
        runs = await self.store.list_runs()
        for run in sorted(runs, key=lambda r: r.created_at):
            if run.status == RUN_RUNNING:
                await self._pump(run.id)

    async def _pump(self, run_id: str) -> None:
        """Settle the run: finish it if it is done, otherwise dispatch what it can."""
        finished = False
        async with self._lock(run_id):
            try:
                run = await self.store.get_run(run_id)
            except RunNotFound:
                return
            if run.status != RUN_RUNNING:
                return  # already terminal, or never started

            steps = await self.store.list_steps(run_id)
            if not steps:
                return

            if any(st.status == STEP_FAILED for st in steps):
                # One bad step ends the run. Steps already running on other
                # devices will report in later and find the run terminal.
                await self.store.finish_run(run_id, RUN_FAILED)
                log.info("run %s failed", run_id)
                finished = True
            elif all(st.status == STEP_COMPLETED for st in steps):
                await self.store.finish_run(run_id, RUN_COMPLETED)
                log.info("run %s completed", run_id)
                finished = True
            else:
                await self._dispatch_ready(steps)

        if finished:
            # Safe outside the lock: a waiter may already hold it, but it will
            # see a terminal run and return without writing anything.
            self._locks.pop(run_id, None)

    async def _dispatch_ready(self, steps: list[Step]) -> None:
        """Offer every step whose dependencies are met to its driver."""
        by_name = {st.name: st for st in steps}

        # Devices this run is already occupying. Offering them a second step
        # would only earn a refusal, so skip rather than churn.
        occupied = {st.device_id for st in steps if st.status == STEP_DISPATCHED}

        ready = [
            st
            for st in steps
            if st.status == STEP_PENDING
            and all(by_name[dep].status == STEP_COMPLETED for dep in st.depends_on)
        ]

        # When two steps are ready and want the same instrument, the one with
        # more work stacked behind it goes first. On the default workflow
        # fill_reagent_plate and fill_buffer_plate become ready together and
        # both need the liquid handler; reagent is followed by warm_reagent,
        # so taking buffer first idles the incubator and costs 2s of a 10s run.
        # Ordering by name -- which is what list_steps returns -- picks buffer.
        depth = self._downstream_depth(steps)
        ready.sort(key=lambda st: (-depth[st.name], st.name))

        for step in ready:
            if step.device_id in occupied:
                continue
            if await self._offer(step):
                occupied.add(step.device_id)

    @staticmethod
    def _downstream_depth(steps: list[Step]) -> dict[str, int]:
        """Longest chain of steps that cannot start until each step is done.

        A count of steps, not of time: every instrument here takes the same
        2s, and nothing in the schema estimates a duration. With real
        durations this would weight by them instead.
        """
        dependents: dict[str, list[str]] = {st.name: [] for st in steps}
        for st in steps:
            for dep in st.depends_on:
                dependents[dep].append(st.name)

        memo: dict[str, int] = {}

        def depth(name: str) -> int:
            # Definitions are cycle-checked at load, so this always terminates.
            if name not in memo:
                memo[name] = 1 + max((depth(d) for d in dependents[name]), default=0)
            return memo[name]

        return {st.name: depth(st.name) for st in steps}

    async def _offer(self, step: Step) -> bool:
        """Claim a step and send it. True if the driver took it."""
        if not await self.store.claim_step(step.id):
            # The step is no longer 'pending', so it is not ours to send.
            # Holding the per-run lock, nothing in this process can reach that
            # state between the snapshot and here -- this guard earns its keep
            # only without the lock, or with the executor replicated.
            return False

        cmd = StepCommand(
            run_id=step.run_id,
            step_id=step.id,
            step_name=step.name,
            device_id=step.device_id,
        )

        try:
            ack = await self.bus.send_command(cmd)
        except BusError as exc:
            # The driver is unreachable. Put the step back; the run will retry
            # on the next result. If nothing else is in flight there is nothing
            # to retry on -- see NOTES.md.
            log.error("could not offer %s to %s: %s", step.name, step.device_id, exc)
            await self.store.release_step(step.id)
            return False

        if not ack.accepted:
            # Normal: the instrument is busy. Put it back so a later pump picks
            # it up -- whatever is occupying that device will report in, and
            # that result pumps this run again.
            log.info(
                "%s refused by %s (%s) -- returned to the pool",
                step.name,
                step.device_id,
                ack.reason,
            )
            await self.store.release_step(step.id)
            return False

        log.info("dispatched %s to %s", step.name, step.device_id)
        return True

    def _lock(self, run_id: str) -> asyncio.Lock:
        lock = self._locks.get(run_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[run_id] = lock
        return lock