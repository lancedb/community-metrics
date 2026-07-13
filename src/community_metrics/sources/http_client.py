from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

COMMUNITY_METRICS_USER_AGENT = (
    "lancedb-community-metrics/0.1 (+https://github.com/lancedb)"
)
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


def build_retrying_session(
    *,
    user_agent: str = COMMUNITY_METRICS_USER_AGENT,
    total_retries: int = 5,
    backoff_factor: float = 1.0,
) -> requests.Session:
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=RETRYABLE_STATUS_CODES,
        backoff_factor=backoff_factor,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
