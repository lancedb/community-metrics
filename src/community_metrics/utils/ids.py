from __future__ import annotations

from datetime import timezone
from uuid import uuid4

from .time import utc_now


def new_ingestion_run_id(job_name: str) -> str:
    ts = utc_now().astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{job_name}:{ts}:{uuid4().hex[:8]}"
