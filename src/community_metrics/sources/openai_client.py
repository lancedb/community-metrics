from __future__ import annotations

from typing import Any

import requests

from community_metrics.config import OPENAI_TIMEOUT_SECONDS


class OpenAIResponsesClient:
    base_url = "https://api.openai.com/v1/responses"

    def __init__(
        self,
        *,
        api_key: str,
        timeout_seconds: int = OPENAI_TIMEOUT_SECONDS,
    ) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def create_structured_response(
        self,
        *,
        model: str,
        reasoning_effort: str,
        instructions: str,
        payload: dict[str, Any],
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        response = requests.post(
            self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "reasoning": {"effort": reasoning_effort},
                "input": [
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "input_text",
                                "text": instructions,
                            }
                        ],
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text",
                                "text": _to_json_text(payload),
                            }
                        ],
                    },
                ],
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "signal_guidance",
                        "schema": schema,
                        "strict": True,
                    }
                },
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()


def _to_json_text(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, indent=2, sort_keys=True, default=str)
