import logging

from .bus import Bus, StepResult
from .store import Store

log = logging.getLogger(__name__)


class Scheduler:
    """The part you need to build.

    It owns the question "what should be running right now, and on what?". The
    executor gives it two entry points:

      - start is called when someone starts a run.
      - handle_result is called every time a driver reports a step finished.
        These may be called concurrently.

    What it has to do:

      - Run every step of the DAG exactly once, respecting depends_on.
      - Run independent branches at the same time. A run that could take 10
        seconds should not take 14.
      - Respect the drivers. A driver does one thing at a time and will refuse a
        command while it is busy (CommandAck.accepted is False). Refusals are not
        fatal, but a step that gets refused and forgotten stalls the run.
      - Move the run to completed, or failed if a step fails.

    Check your work with ./scripts/acceptance.sh.

    The Store and Bus are yours to extend -- add methods, change the schema in
    db/init.sql, whatever you need. Nothing outside this file has to stay as it
    is.
    """

    def __init__(self, store: Store, bus: Bus) -> None:
        self.store = store
        self.bus = bus

    async def start(self, run_id: str) -> None:
        """Begin executing a run."""
        await self.store.start_run(run_id)

        # TODO: work out which steps can run now, and get them going.
        log.info("scheduler: run %s started, but nothing is scheduled yet", run_id)

    async def handle_result(self, result: StepResult) -> None:
        """Record that a driver finished a step and move the run on.

        May be called concurrently.
        """
        # TODO: record the result, then work out what can run now.
        log.info(
            "scheduler: driver %s reported %s finished, and nothing happened",
            result.device_id,
            result.step_name,
        )
