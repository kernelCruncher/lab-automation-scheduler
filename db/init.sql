-- Lab automation: workflow execution.
--
-- The executor owns all run and step state. Workers are stateless: they receive
-- a step, do the physical work, and report back.

CREATE DATABASE executor_db;

\c executor_db

CREATE TABLE devices (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    type       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A run is one execution of a workflow.
-- status: pending | running | completed | failed | aborted
CREATE TABLE runs (
    id            TEXT PRIMARY KEY,
    workflow_name TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One node of the workflow DAG, for one run.
-- depends_on holds the names of steps in the same run that must finish first.
-- status: pending | dispatched | running | completed | failed
CREATE TABLE steps (
    id            TEXT PRIMARY KEY,
    run_id        TEXT        NOT NULL REFERENCES runs(id),
    name          TEXT        NOT NULL,
    device_id     TEXT        NOT NULL REFERENCES devices(id),
    status        TEXT        NOT NULL DEFAULT 'pending',
    depends_on     TEXT[]     NOT NULL DEFAULT '{}',
    dispatch_count INT        NOT NULL DEFAULT 0,
    dispatched_at TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    error         TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, name)
);

CREATE INDEX idx_steps_run    ON steps(run_id);
CREATE INDEX idx_steps_status ON steps(status);

INSERT INTO devices (id, name, type) VALUES
    ('liquid-handler-1', 'Liquid Handler 1', 'liquid_handler'),
    ('incubator-1',      'Incubator 1',      'incubator'),
    ('plate-reader-1',   'Plate Reader 1',   'plate_reader');
