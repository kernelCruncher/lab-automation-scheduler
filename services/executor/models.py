from dataclasses import dataclass
from datetime import datetime

RUN_PENDING = "pending"
RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_ABORTED = "aborted"

STEP_PENDING = "pending"
STEP_DISPATCHED = "dispatched"
STEP_RUNNING = "running"
STEP_COMPLETED = "completed"
STEP_FAILED = "failed"


@dataclass(frozen=True)
class Device:
    id: str
    name: str
    type: str


@dataclass(frozen=True)
class Run:
    id: str
    workflow_name: str
    status: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class Step:
    id: str
    run_id: str
    name: str
    device_id: str
    status: str
    depends_on: list[str]
    dispatch_count: int
    dispatched_at: datetime | None
    finished_at: datetime | None
    error: str | None
