import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NATSTimeoutError

log = logging.getLogger(__name__)

SUBJECT_RESULT = "steps.result"
COMMAND_TIMEOUT = 3.0


def command_subject(device_id: str) -> str:
    return f"drivers.{device_id}.command"


def state_subject(device_id: str) -> str:
    return f"drivers.{device_id}.state"


@dataclass(frozen=True)
class StepCommand:
    """Tells a driver to run one step.

    Sending it is a request: the driver answers immediately with a CommandAck
    saying whether it took the work. The step finishing is reported separately,
    later, as a StepResult.
    """

    run_id: str
    step_id: str
    step_name: str
    device_id: str


@dataclass(frozen=True)
class CommandAck:
    """The driver's immediate answer to a StepCommand.

    A driver that is already working refuses the command.
    """

    accepted: bool
    reason: str = ""


@dataclass(frozen=True)
class StepResult:
    """A driver reporting that it has finished. error is empty on success."""

    run_id: str
    step_id: str
    step_name: str
    device_id: str
    error: str = ""


@dataclass(frozen=True)
class DriverState:
    """What a driver says about itself."""

    device_id: str
    busy: bool = False
    current_step: str = ""
    executed: list[str] = field(default_factory=list)
    rejected: int = 0
    drop_result_pct: int = 0
    dropped: int = 0
    fail_pct: int = 0
    failed: int = 0


class BusError(Exception):
    """The bus could not complete a request."""


ResultHandler = Callable[[StepResult], Coroutine[Any, Any, None]]


class Bus(Protocol):
    """How the executor talks to the drivers."""

    async def send_command(self, cmd: StepCommand) -> CommandAck:
        """Offer a step to a driver and wait for its immediate answer.

        A driver that is busy returns accepted=False. This does NOT wait for the
        step to finish.
        """
        ...

    async def on_step_result(self, handler: ResultHandler) -> None:
        """Register a handler for drivers reporting completion.

        Handlers may be called concurrently.
        """
        ...

    async def driver_state(self, device_id: str) -> DriverState:
        """Ask a driver what it is doing.

        Not required to build a working executor -- it is there if you want it.
        """
        ...

    async def close(self) -> None: ...


class NATSBus:
    def __init__(self, nc: NATSClient) -> None:
        self._nc = nc
        # Tasks are kept alive here: asyncio only holds a weak reference to a
        # running task, so a task with no strong reference can be collected
        # mid-flight.
        self._tasks: set[asyncio.Task[None]] = set()

    @classmethod
    async def connect(cls, url: str) -> "NATSBus":
        try:
            nc = await nats.connect(url, max_reconnect_attempts=-1, reconnect_time_wait=1)
        except OSError as exc:
            raise BusError(f"connect nats {url}: {exc}") from exc
        return cls(nc)

    async def send_command(self, cmd: StepCommand) -> CommandAck:
        payload = json.dumps(asdict(cmd)).encode()
        try:
            msg = await self._nc.request(
                command_subject(cmd.device_id), payload, timeout=COMMAND_TIMEOUT
            )
        except (NATSTimeoutError, NoRespondersError) as exc:
            raise BusError(f"command {cmd.step_name} to {cmd.device_id}: {exc!r}") from exc

        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError as exc:
            raise BusError(f"bad ack from {cmd.device_id}: {exc}") from exc
        return CommandAck(accepted=bool(data.get("accepted", False)), reason=data.get("reason", ""))

    async def on_step_result(self, handler: ResultHandler) -> None:
        async def on_message(msg: Msg) -> None:
            try:
                data = json.loads(msg.data)
                result = StepResult(
                    run_id=data["run_id"],
                    step_id=data["step_id"],
                    step_name=data["step_name"],
                    device_id=data["device_id"],
                    error=data.get("error") or "",
                )
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                log.error("bus: bad result payload %r: %s", msg.data, exc)
                return

            # Each result is handed to its own task so that one slow handler
            # cannot hold up the rest of the bus. This is a choice, and it means
            # your handler can be called concurrently.
            #
            # If you would rather results arrived one at a time, this is the line
            # to change -- `await handler(result)` instead. Either is defensible
            # -- tell us which you picked and why.
            task = asyncio.create_task(handler(result))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            task.add_done_callback(lambda t: self._log_handler_failure(result, t))

        await self._nc.subscribe(SUBJECT_RESULT, cb=on_message)

    @staticmethod
    def _log_handler_failure(result: StepResult, task: "asyncio.Task[None]") -> None:
        """A task that raises is otherwise silent, and the run just stalls."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.error(
                "bus: result handler for %s (step=%s run=%s device=%s) raised: %r",
                result.step_name,
                result.step_id,
                result.run_id,
                result.device_id,
                exc,
            )

    async def driver_state(self, device_id: str) -> DriverState:
        try:
            msg = await self._nc.request(state_subject(device_id), b"", timeout=COMMAND_TIMEOUT)
        except (NATSTimeoutError, NoRespondersError) as exc:
            raise BusError(f"state of {device_id}: {exc!r}") from exc

        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError as exc:
            raise BusError(f"bad state from {device_id}: {exc}") from exc

        return DriverState(
            device_id=data.get("device_id", device_id),
            busy=bool(data.get("busy", False)),
            current_step=data.get("current_step") or "",
            executed=list(data.get("executed") or []),
            rejected=int(data.get("rejected", 0)),
            drop_result_pct=int(data.get("drop_result_pct", 0)),
            dropped=int(data.get("dropped", 0)),
            fail_pct=int(data.get("fail_pct", 0)),
            failed=int(data.get("failed", 0)),
        )

    async def close(self) -> None:
        await self._nc.close()
