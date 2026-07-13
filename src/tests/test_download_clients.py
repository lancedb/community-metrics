from __future__ import annotations

from dataclasses import dataclass

from community_metrics.sources.crates_client import CratesClient
from community_metrics.sources.http_client import (
    COMMUNITY_METRICS_USER_AGENT,
    RETRYABLE_STATUS_CODES,
    build_retrying_session,
)


@dataclass
class _Response:
    payload: dict[str, object]

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self.payload


class _Session:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, *, timeout: int) -> _Response:
        self.calls.append((url, timeout))
        return _Response(self.payload)


def test_retrying_session_retries_throttling_and_transient_server_errors() -> None:
    session = build_retrying_session(total_retries=4, backoff_factor=2.0)

    retry = session.adapters["https://"].max_retries
    assert retry.total == 4
    assert retry.backoff_factor == 2.0
    assert retry.respect_retry_after_header is True
    assert set(retry.status_forcelist) == set(RETRYABLE_STATUS_CODES)
    assert session.headers["User-Agent"] == COMMUNITY_METRICS_USER_AGENT


def test_crates_client_sends_policy_compliant_user_agent_and_parses_totals() -> None:
    session = _Session(
        {
            "version_downloads": [
                {"date": "2026-07-10", "downloads": 7},
            ],
            "meta": {
                "extra_downloads": [
                    {"date": "2026-07-10", "downloads": 3},
                    {"date": "2026-07-11", "downloads": 5},
                ]
            },
        }
    )
    client = CratesClient(
        timeout_seconds=9,
        session=session,  # type: ignore[arg-type]
        min_request_interval_seconds=0,
    )

    rows = client.fetch_daily_downloads("lance")

    assert session.headers["User-Agent"] == COMMUNITY_METRICS_USER_AGENT
    assert session.calls == [("https://crates.io/api/v1/crates/lance/downloads", 9)]
    assert [(row.day, row.downloads) for row in rows] == [
        ("2026-07-10", 10),
        ("2026-07-11", 5),
    ]
