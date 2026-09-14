"""This test forces the situation the drivers cannot reliably produce: two steps
finishing at the exact same instant. It exists because real instruments finish
milliseconds apart, which is long enough to hide a scheduling race.

It only uses Store, Scheduler, start and handle_result. Everything inside those
is yours.

Run it with:  docker compose run --rm tests
"""

import asyncio
import os

import pytest
from psycopg_pool import AsyncConnectionPool

from services.executor.bus import CommandAck, DriverState, ResultHandler, StepCommand, StepResult
from services.executor.models import STEP_COMPLETED
from services.executor.scheduler import Scheduler
from services.executor.store import Store
from services.executor.workflows import StepTemplate, load_workflows

# The scenario is repeated because the outcome depends on how two tasks
# interleave across a couple of database round trips. One attempt is not enough
# to be sure.
ATTEMPTS = 10


class RecordingBus:
    """Accepts every command and remembers what it was asked to send."""

    def __init__(self) -> None:
        self._commands: list[StepCommand] = []

    async def send_command(self, cmd: StepCommand) -> CommandAck:
        self._commands.append(cmd)
        return CommandAck(accepted=True)

    async def on_step_result(self, handler: ResultHandler) -> None:
        return None

    async def driver_state(self, device_id: str) -> DriverState:
        return DriverState(device_id=device_id)

    async def close(self) -> None:
        return None

    def sent(self) -> list[StepCommand]:
        return list(self._commands)


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set -- run with: docker compose run --rm tests")
    return url


@pytest.fixture
async def pool(database_url: str):
    async with AsyncConnectionPool(database_url, min_size=2, max_size=10, open=False) as p:
        await p.wait()
        yield p


async def test_simultaneous_results_dispatch_each_step_once(pool) -> None:
    path = os.getenv("WORKFLOWS_FILE", "/src/config/workflows.yaml")
    wf_set = load_workflows(path)
    name = wf_set.default
    template = wf_set.get(name)

    store = Store(pool)
    for attempt in range(1, ATTEMPTS + 1):
        await run_one_attempt(store, name, template, attempt)


async def run_one_attempt(
    store: Store, name: str, template: list[StepTemplate], attempt: int
) -> None:
    run = await store.create_run(name, template)

    bus = RecordingBus()
    sched = Scheduler(store, bus)
    await sched.start(run.id)

    # Stand in for the drivers. Every time more than one step is in flight,
    # report them all finished at the same moment.
    completed: set[str] = set()
    for _ in range(20):
        batch = [c for c in bus.sent() if c.step_id not in completed]
        if not batch:
            break
        completed.update(c.step_id for c in batch)

        release = asyncio.Barrier(len(batch))

        async def report(cmd: StepCommand, barrier: asyncio.Barrier) -> None:
            await barrier.wait()  # everyone starts together
            await sched.handle_result(
                StepResult(
                    run_id=cmd.run_id,
                    step_id=cmd.step_id,
                    step_name=cmd.step_name,
                    device_id=cmd.device_id,
                )
            )

        await asyncio.gather(*(report(c, release) for c in batch))
        await asyncio.sleep(0.02)  # let any trailing work settle

    # Every step should have been commanded exactly once.
    counts: dict[str, int] = {}
    names: dict[str, str] = {}
    for cmd in bus.sent():
        counts[cmd.step_id] = counts.get(cmd.step_id, 0) + 1
        names[cmd.step_id] = cmd.step_name

    duplicates = {names[sid]: n for sid, n in counts.items() if n > 1}
    assert not duplicates, (
        f"attempt {attempt}: {duplicates} -- two results both decided a step was "
        f"runnable and both sent it"
    )

    steps = await store.list_steps(run.id)
    unfinished = {st.name: st.status for st in steps if st.status != STEP_COMPLETED}
    assert not unfinished, f"attempt {attempt}: {unfinished} did not end as completed"
