import os
from pathlib import Path

from community_metrics import config


def test_load_env_file_parses_export_and_quotes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("COMMUNITY_METRICS_TIMEOUT_SECONDS", raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "export GITHUB_TOKEN='abc123'",
                'COMMUNITY_METRICS_TIMEOUT_SECONDS="45"',
            ]
        ),
        encoding="utf-8",
    )

    config._load_env_file(env_file)

    assert os.getenv("GITHUB_TOKEN") == "abc123"
    assert os.getenv("COMMUNITY_METRICS_TIMEOUT_SECONDS") == "45"


def test_load_env_file_does_not_override_existing_env(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "from-shell")

    env_file = tmp_path / ".env"
    env_file.write_text("GITHUB_TOKEN=from-file\n", encoding="utf-8")

    config._load_env_file(env_file)

    assert os.getenv("GITHUB_TOKEN") == "from-shell"
