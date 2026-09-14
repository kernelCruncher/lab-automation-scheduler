import secrets
from collections.abc import Sequence
from typing import Any

from psycopg_pool import AsyncConnectionPool

from .models import (
    RUN_PENDING,
    RUN_RUNNING,
    STEP_DISPATCHED,
    STEP_PENDING,
    Device,
    Run,
    Step,
)
from .workflows import StepTemplate


class RunNotFound(Exception):
    """No run with that id."""


def new_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


RUN_COLS = "id, workflow_name, status, created_at, started_at, finished_at"
STEP_COLS = (
    "id, run_id, name, device_id, status, depends_on, "
    "dispatch_count, dispatched_at, finished_at, error"
)


def _run(row: Sequence[Any]) -> Run:
    return Run(*row)


def _step(row: Sequence[Any]) -> Step:
    return Step(*row)


class Store:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def list_devices(self) -> list[Device]:
        async with self._pool.connection() as conn:
            cur = await conn.execute("SELECT id, name, type FROM devices ORDER BY id")
            return [Device(*row) for row in await cur.fetchall()]

    async def list_runs(self) -> list[Run]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(f"SELECT {RUN_COLS} FROM runs ORDER BY created_at DESC")
            return [_run(row) for row in await cur.fetchall()]

    async def get_run(self, run_id: str) -> Run:
        async with self._pool.connection() as conn:
            cur = await conn.execute(f"SELECT {RUN_COLS} FROM runs WHERE id = %s", (run_id,))
            row = await cur.fetchone()
        if row is None:
            raise RunNotFound(run_id)
        return _run(row)

    async def create_run(self, workflow_name: str, template: list[StepTemplate]) -> Run:
        """Materialise a workflow template into a run and its steps."""
        run_id = new_id("run")
        # The pool's context manager commits on exit and rolls back on error, so
        # a run is never left without its steps.
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"INSERT INTO runs (id, workflow_name) VALUES (%s, %s) RETURNING {RUN_COLS}",
                (run_id, workflow_name),
            )
            row = await cur.fetchone()
            for st in template:
                await conn.execute(
                    "INSERT INTO steps (id, run_id, name, device_id, depends_on)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (new_id("step"), run_id, st.name, st.device_id, list(st.depends_on)),
                )
        assert row is not None  # RETURNING on a successful INSERT always yields a row
        return _run(row)

    async def list_steps(self, run_id: str) -> list[Step]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"SELECT {STEP_COLS} FROM steps WHERE run_id = %s ORDER BY name",
                (run_id,),
            )
            return [_step(row) for row in await cur.fetchall()]

    async def start_run(self, run_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = %s, started_at = now(), updated_at = now()"
                " WHERE id = %s AND status = %s",
                (RUN_RUNNING, run_id, RUN_PENDING),
            )

    async def finish_run(self, run_id: str, status: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = %s, finished_at = now(), updated_at = now()"
                " WHERE id = %s",
                (status, run_id),
            )

    async def record_step_dispatched(self, step_id: str) -> None:
        """Note that a step has been sent to its driver."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps"
                "   SET status = %s,"
                "       dispatch_count = dispatch_count + 1,"
                "       dispatched_at = COALESCE(dispatched_at, now()),"
                "       updated_at = now()"
                " WHERE id = %s",
                (STEP_DISPATCHED, step_id),
            )

    # The two halves of taking a step out of the pool and putting it back.
    # The status names are close enough to be worth spelling out:
    #
    #   pending     nobody owns it -- either just created, or handed back after
    #               a driver refused it. A candidate for dispatch, which is not
    #               the same as ready: its dependencies may be unfinished.
    #   dispatched  claimed here and accepted by a driver, which is working on
    #               it now. Not a candidate; leave it alone until it reports.
    #
    # claim_step is called immediately before send_command, release_step only
    # when that command is refused or never lands. Between the two a step reads
    # as dispatched while no instrument holds it -- a few milliseconds, all of
    # it inside the scheduler's per-run lock.

    async def claim_step(self, step_id: str) -> bool:
        """Take exclusive ownership of a pending step, atomically.

        The WHERE clause is the arbiter: Postgres applies the two concurrent
        UPDATEs one after the other, so the second finds no row in 'pending'
        and updates nothing. Returns True only for the caller that won, which
        is therefore the only one that may send the command.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "UPDATE steps"
                "   SET status = %s,"
                "       dispatch_count = dispatch_count + 1,"
                "       dispatched_at = now(),"
                "       updated_at = now()"
                " WHERE id = %s AND status = %s"
                " RETURNING id",
                (STEP_DISPATCHED, step_id, STEP_PENDING),
            )
            return await cur.fetchone() is not None

    async def release_step(self, step_id: str) -> None:
        """Return a claimed step to the pool after a driver refused it.

        dispatched_at is cleared so the timeline records when the step actually
        occupied its device, not when we first offered it. dispatch_count is
        left alone -- the attempt is worth counting.
        """
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps SET status = %s, dispatched_at = NULL, updated_at = now()"
                " WHERE id = %s AND status = %s",
                (STEP_PENDING, step_id, STEP_DISPATCHED),
            )

    async def record_step_finished(self, step_id: str, status: str, error: str = "") -> None:
        """Note that a step finished, successfully or not."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps SET status = %s, error = %s,"
                " finished_at = now(), updated_at = now()"
                " WHERE id = %s",
                (status, error or None, step_id),
            )