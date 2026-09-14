from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class StepTemplate:
    """One node of the DAG.

    Names the device that must run it and the steps within the same run that
    have to finish first.
    """

    name: str
    device_id: str
    depends_on: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class WorkflowTemplate:
    name: str
    steps: list[StepTemplate]


class WorkflowError(Exception):
    """A workflow definition could not be read or could not run."""


class WorkflowSet:
    def __init__(self, default: str, workflows: list[WorkflowTemplate]) -> None:
        self._default = default
        self._by_name = {wf.name: wf for wf in workflows}
        self._name_order = [wf.name for wf in workflows]

    def get(self, name: str) -> list[StepTemplate]:
        wf = self._by_name.get(name)
        if wf is None:
            raise WorkflowError(f"unknown workflow {name!r} (have: {self._name_order})")
        return wf.steps

    @property
    def default(self) -> str:
        return self._default

    @property
    def names(self) -> list[str]:
        return list(self._name_order)


def load_workflows(path: str | Path) -> WorkflowSet:
    """Read and validate the workflow definitions."""
    try:
        raw = Path(path).read_text()
    except OSError as exc:
        raise WorkflowError(f"read {path}: {exc}") from exc

    try:
        file: Any = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise WorkflowError(f"parse {path}: {exc}") from exc

    if not isinstance(file, dict) or not file.get("workflows"):
        raise WorkflowError(f"{path} defines no workflows")

    workflows: list[WorkflowTemplate] = []
    seen_names: set[str] = set()
    for entry in file["workflows"]:
        name = (entry or {}).get("name") or ""
        if not name:
            raise WorkflowError("a workflow is missing its name")
        if name in seen_names:
            raise WorkflowError(f"workflow {name!r} is defined twice")
        seen_names.add(name)

        steps = [
            StepTemplate(
                name=(st or {}).get("name") or "",
                device_id=(st or {}).get("device") or "",
                depends_on=list((st or {}).get("depends_on") or []),
            )
            for st in (entry.get("steps") or [])
        ]
        wf = WorkflowTemplate(name=name, steps=steps)
        try:
            _validate(wf)
        except WorkflowError as exc:
            raise WorkflowError(f"workflow {name!r}: {exc}") from exc
        workflows.append(wf)

    default = file.get("default") or workflows[0].name
    if default not in seen_names:
        raise WorkflowError(f"default workflow {default!r} is not defined")
    return WorkflowSet(default, workflows)


def _validate(wf: WorkflowTemplate) -> None:
    """Reject definitions that could not run.

    Duplicate or missing step names, dependencies that do not exist, and cycles.
    """
    if not wf.steps:
        raise WorkflowError("has no steps")

    seen: set[str] = set()
    for st in wf.steps:
        if not st.name:
            raise WorkflowError("a step is missing its name")
        if st.name in seen:
            raise WorkflowError(f"step {st.name!r} is defined twice")
        if not st.device_id:
            raise WorkflowError(f"step {st.name!r} is missing its device")
        seen.add(st.name)

    for st in wf.steps:
        for dep in st.depends_on:
            if dep == st.name:
                raise WorkflowError(f"step {st.name!r} depends on itself")
            if dep not in seen:
                raise WorkflowError(f"step {st.name!r} depends on {dep!r}, which is not defined")

    _detect_cycle(wf.steps)


def _detect_cycle(steps: list[StepTemplate]) -> None:
    deps = {st.name: st.depends_on for st in steps}

    UNVISITED, IN_STACK, DONE = 0, 1, 2
    state: dict[str, int] = {}

    def walk(name: str, path: list[str]) -> None:
        if state.get(name, UNVISITED) == IN_STACK:
            raise WorkflowError(f"dependency cycle: {path} -> {name}")
        if state.get(name, UNVISITED) == DONE:
            return
        state[name] = IN_STACK
        for dep in deps.get(name, []):
            walk(dep, [*path, name])
        state[name] = DONE

    for st in steps:
        walk(st.name, [])
