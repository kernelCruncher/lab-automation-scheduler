"""A driver stands in for one piece of lab equipment.

It does one thing at a time. Offered a step while it is already working, it
refuses -- real instruments do not politely queue. Ask it for its state and it
will tell you what it is doing and what it has done.

You should not need to change this.
"""

import asyncio
import json
import logging
import os
import random
import re
import signal
import sys
from dataclasses import asdict, dataclass, field

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

log = logging.getLogger("driver")

SUBJECT_RESULT = "steps.result"

_DURATION_UNITS = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}
_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")


@dataclass(frozen=True)
class StepCommand:
    run_id: str
    step_id: str
    step_name: str
    device_id: str


@dataclass(frozen=True)
class CommandAck:
    accepted: bool
    reason: str = ""


@dataclass(frozen=True)
class StepResult:
    run_id: str
    step_id: str
    step_name: str
    device_id: str
    error: str = ""


@dataclass
class DriverState:
    device_id: str
    busy: bool = False
    current_step: str = ""
    executed: list[str] = field(default_factory=list)
    rejected: int = 0
    drop_result_pct: int = 0
    dropped: int = 0
    fail_pct: int = 0
    failed: int = 0


def parse_duration(text: str) -> float:
    """Parse a Go duration such as '2s' or '1m30s' into seconds.

    docker-compose.yml sets STEP_DURATION in Go's format, and the brief
    documents it, so the value is parsed rather than changed.
    """
    parts = _DURATION_PART.findall(text.strip())
    if not parts or "".join(n + u for n, u in parts) != text.strip():
        raise ValueError(f"STEP_DURATION is not a duration: {text!r}")
    return sum(float(n) * _DURATION_UNITS[u] for n, u in parts)


class Driver:
    def __init__(
        self,
        device_id: str,
        nc: NATSClient,
        duration: float,
        drop_pct: int,
        fail_pct: int,
    ) -> None:
        self.nc = nc
        self.duration = duration
        self.state = DriverState(device_id=device_id, drop_result_pct=drop_pct, fail_pct=fail_pct)
        self._tasks: set[asyncio.Task[None]] = set()

    async def handle_command(self, msg: Msg) -> None:
        """Answer straight away, then do the work in the background."""
        try:
            data = json.loads(msg.data)
            cmd = StepCommand(
                run_id=data["run_id"],
                step_id=data["step_id"],
                step_name=data["step_name"],
                device_id=data["device_id"],
            )
        except (json.JSONDecodeError, KeyError, TypeError):
            await self.reply(msg, CommandAck(accepted=False, reason="malformed command"))
            return

        # There is no await between the check and the set, so the event loop
        # cannot interleave another command here. That is why no lock is needed.
        if self.state.busy:
            self.state.rejected += 1
            busy_with = self.state.current_step
            log.info("REFUSED %s: already running %s", cmd.step_name, busy_with)
            await self.reply(msg, CommandAck(accepted=False, reason=f"busy with {busy_with}"))
            return

        self.state.busy = True
        self.state.current_step = cmd.step_name
        self.state.executed.append(cmd.step_id)

        log.info("accepted %s (step=%s run=%s)", cmd.step_name, cmd.step_id, cmd.run_id)
        await self.reply(msg, CommandAck(accepted=True))

        task = asyncio.create_task(self.execute(cmd))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def execute(self, cmd: StepCommand) -> None:
        await asyncio.sleep(self.duration)

        self.state.busy = False
        self.state.current_step = ""

        # Did the step itself go wrong? See FAIL_PCT.
        error = ""
        if self.state.fail_pct > 0 and random.randrange(100) < self.state.fail_pct:
            error = f"instrument error during {cmd.step_name}"
            self.state.failed += 1
            log.info("FAILED %s -- reporting an error", cmd.step_name)

        # The instrument has finished, one way or the other. Whether anyone hears
        # about it is a separate question -- see DROP_RESULT_PCT.
        if self.state.drop_result_pct > 0 and random.randrange(100) < self.state.drop_result_pct:
            self.state.dropped += 1
            log.info(
                "DROPPED the result for %s -- the work was done, the report was not sent",
                cmd.step_name,
            )
            return

        result = StepResult(
            run_id=cmd.run_id,
            step_id=cmd.step_id,
            step_name=cmd.step_name,
            device_id=cmd.device_id,
            error=error,
        )
        try:
            await self.nc.publish(SUBJECT_RESULT, json.dumps(asdict(result)).encode())
        except OSError as exc:
            log.error("publish result for %s: %s", cmd.step_name, exc)
            return
        if not error:
            log.info("finished %s", cmd.step_name)

    async def handle_state(self, msg: Msg) -> None:
        payload = asdict(self.state)
        payload["executed"] = list(self.state.executed)
        try:
            await msg.respond(json.dumps(payload).encode())
        except OSError as exc:
            log.error("respond state: %s", exc)

    async def reply(self, msg: Msg, ack: CommandAck) -> None:
        try:
            await msg.respond(json.dumps(asdict(ack)).encode())
        except OSError as exc:
            log.error("respond ack: %s", exc)


def _pct(name: str) -> int:
    raw = os.getenv(name)
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        sys.exit(f"{name} must be a whole number from 0 to 100, got {raw!r}")
    if not 0 <= value <= 100:
        sys.exit(f"{name} must be a whole number from 0 to 100, got {raw!r}")
    return value


async def run() -> None:
    worker_id = os.getenv("WORKER_ID")
    if not worker_id:
        sys.exit("WORKER_ID environment variable is required")
    nats_url = os.getenv("NATS_URL")
    if not nats_url:
        sys.exit("NATS_URL environment variable is required")

    duration = 2.0
    if raw := os.getenv("STEP_DURATION"):
        try:
            duration = parse_duration(raw)
        except ValueError as exc:
            sys.exit(str(exc))

    drop_pct = _pct("DROP_RESULT_PCT")
    fail_pct = _pct("FAIL_PCT")

    try:
        nc = await nats.connect(nats_url, max_reconnect_attempts=-1, reconnect_time_wait=1)
    except OSError as exc:
        sys.exit(f"Failed to connect to the bus: {exc}")

    driver = Driver(worker_id, nc, duration, drop_pct, fail_pct)
    await nc.subscribe(f"drivers.{worker_id}.command", cb=driver.handle_command)
    await nc.subscribe(f"drivers.{worker_id}.state", cb=driver.handle_state)

    step = f"{duration:g}s"
    if drop_pct and fail_pct:
        log.info(
            "driver %s ready (step duration %s, FAILING %d%%, DROPPING %d%% of results)",
            worker_id,
            step,
            fail_pct,
            drop_pct,
        )
    elif drop_pct:
        log.info(
            "driver %s ready (step duration %s, DROPPING %d%% of results)",
            worker_id,
            step,
            drop_pct,
        )
    elif fail_pct:
        log.info(
            "driver %s ready (step duration %s, FAILING %d%% of steps)",
            worker_id,
            step,
            fail_pct,
        )
    else:
        log.info("driver %s ready (step duration %s)", worker_id, step)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    log.info("driver %s shutting down", worker_id)
    await nc.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
