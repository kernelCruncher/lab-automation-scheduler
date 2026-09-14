import logging
import os
import sys

import uvicorn

from .api import create_app
from .workflows import WorkflowError, load_workflows

log = logging.getLogger(__name__)

PORT = 5001
DEFAULT_WORKFLOWS_FILE = "/app/config/workflows.yaml"


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        sys.exit(f"{name} environment variable is required")
    return value


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    database_url = _required("DATABASE_URL")
    nats_url = _required("NATS_URL")
    workflows_file = os.getenv("WORKFLOWS_FILE") or DEFAULT_WORKFLOWS_FILE

    # Fail fast if the definitions are broken, rather than at the first run.
    try:
        wf_set = load_workflows(workflows_file)
    except WorkflowError as exc:
        sys.exit(f"Failed to load workflows: {exc}")

    log.info(
        "loaded %d workflow(s) from %s, default %r",
        len(wf_set.names),
        workflows_file,
        wf_set.default,
    )

    app = create_app(
        database_url=database_url,
        nats_url=nats_url,
        workflows_file=workflows_file,
    )

    log.info("Starting executor on :%d", PORT)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
