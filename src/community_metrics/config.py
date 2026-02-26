from __future__ import annotations

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SEED_DATA_DIR = ROOT_DIR / "seed_data"


def _unquote_env_value(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = _unquote_env_value(raw_value)


_load_env_file(ROOT_DIR / ".env")

GITHUB_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip() or None
GITHUB_BACKFILL = os.getenv("GITHUB_BACKFILL", "0") == "1"

LANCEDB_API_KEY = (os.getenv("LANCEDB_API_KEY") or "").strip()
LANCEDB_HOST_OVERRIDE = (os.getenv("LANCEDB_HOST_OVERRIDE") or "").strip()
LANCEDB_REGION = (os.getenv("LANCEDB_REGION") or "us-east-1").strip() or "us-east-1"

REQUEST_TIMEOUT_SECONDS = int(os.getenv("COMMUNITY_METRICS_TIMEOUT_SECONDS", "30"))

DEFAULT_DAYS = 180
MAX_DAYS = 730
