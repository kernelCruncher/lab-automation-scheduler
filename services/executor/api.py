import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from .bus import BusError, NATSBus
from .models import Run, Step
from .scheduler import Scheduler
from .store import RunNotFound, Store
from .workflows import WorkflowError, load_workflows

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimelineEntry:
    name: str
    device_id: str
    status: str
    depends_on: list[str]
    dispatch_count: int
    start_offset_ms: int | None
    end_offset_ms: int | None
    duration_ms: int | None
    dispatched_at: datetime | None
    finished_at: datetime | None


class CreateRunRequest(BaseModel):
    workflow_name: str | None = None


def _error(code: int, msg: str) -> JSONResponse:
    return JSONResponse(status_code=code, content={"error": msg})


def _ms(later: datetime, earlier: datetime) -> int:
    return int((later - earlier).total_seconds() * 1000)


def create_app(*, database_url: str, nats_url: str, workflows_file: str) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = AsyncConnectionPool(database_url, min_size=2, max_size=10, open=False)
        await pool.open()
        await pool.wait()

        bus = await NATSBus.connect(nats_url)
        store = Store(pool)
        scheduler = Scheduler(store, bus)
        await bus.on_step_result(scheduler.handle_result)

        app.state.store = store
        app.state.bus = bus
        app.state.scheduler = scheduler
        app.state.workflows_file = workflows_file
        log.info("executor ready")
        try:
            yield
        finally:
            await bus.close()
            await pool.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.exception_handler(RunNotFound)
    async def _on_run_not_found(request: Request, exc: Exception) -> JSONResponse:
        return _error(404, "run not found")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "healthy"}

    @app.get("/devices")
    async def list_devices(request: Request) -> Any:
        store: Store = request.app.state.store
        return await store.list_devices()

    @app.get("/drivers")
    async def list_driver_states(request: Request) -> Any:
        """Ask every driver what it is doing.

        Useful for checking your work: each driver reports the steps it actually
        executed and how many commands it refused.
        """
        store: Store = request.app.state.store
        bus = request.app.state.bus

        out: list[Any] = []
        for device in await store.list_devices():
            try:
                out.append(await bus.driver_state(device.id))
            except BusError as exc:
                log.warning("could not read state of %s: %s", device.id, exc)
                out.append({"device_id": device.id, "error": str(exc)})
        return out

    @app.get("/workflows")
    async def list_workflows(request: Request) -> Any:
        """Report what is currently defined in workflows.yaml."""
        try:
            wf_set = load_workflows(request.app.state.workflows_file)
        except WorkflowError as exc:
            return _error(500, str(exc))

        return [
            {
                "name": name,
                "is_default": name == wf_set.default,
                "step_count": len(wf_set.get(name)),
                "steps": [st.name for st in wf_set.get(name)],
            }
            for name in wf_set.names
        ]

    @app.get("/runs")
    async def list_runs(request: Request) -> Any:
        store: Store = request.app.state.store
        return await store.list_runs()

    @app.post("/runs", status_code=201)
    async def create_run(request: Request, body: CreateRunRequest | None = None) -> Any:
        store: Store = request.app.state.store

        # Read the definitions fresh so edits to workflows.yaml take effect
        # without a restart.
        try:
            wf_set = load_workflows(request.app.state.workflows_file)
        except WorkflowError as exc:
            return _error(500, str(exc))

        name = (body.workflow_name if body else None) or wf_set.default
        try:
            template = wf_set.get(name)
        except WorkflowError as exc:
            return _error(400, str(exc))

        return await store.create_run(name, template)

    @app.get("/runs/{run_id}")
    async def get_run(request: Request, run_id: str) -> Any:
        store: Store = request.app.state.store
        run: Run = await store.get_run(run_id)
        steps: list[Step] = await store.list_steps(run.id)
        return {"run": run, "steps": steps}

    @app.post("/runs/{run_id}/start")
    async def start_run(request: Request, run_id: str) -> Any:
        store: Store = request.app.state.store
        scheduler: Scheduler = request.app.state.scheduler

        await store.get_run(run_id)
        await scheduler.start(run_id)
        return {"message": "run started", "run_id": run_id}

    @app.get("/runs/{run_id}/timeline")
    async def timeline(request: Request, run_id: str) -> Any:
        """Show when each step actually occupied its device."""
        store: Store = request.app.state.store
        run = await store.get_run(run_id)
        steps = await store.list_steps(run.id)

        origin = run.started_at or run.created_at

        entries = [
            TimelineEntry(
                name=st.name,
                device_id=st.device_id,
                status=st.status,
                depends_on=st.depends_on,
                dispatch_count=st.dispatch_count,
                start_offset_ms=_ms(st.dispatched_at, origin) if st.dispatched_at else None,
                end_offset_ms=_ms(st.finished_at, origin) if st.finished_at else None,
                duration_ms=(
                    _ms(st.finished_at, st.dispatched_at)
                    if st.dispatched_at and st.finished_at
                    else None
                ),
                dispatched_at=st.dispatched_at,
                finished_at=st.finished_at,
            )
            for st in steps
        ]

        return {
            "run_id": run.id,
            "status": run.status,
            "total_duration_ms": _ms(run.finished_at, origin) if run.finished_at else None,
            "steps": entries,
        }

    return app
