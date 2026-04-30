"""Tests for runtime logging utilities."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import pytest

from application.services.runtime_service import configure_logging, env_list, load_env_file


def test_configure_logging_uses_module_name_for_log_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Module-specific logging should write to a matching log filename."""

    monkeypatch.setenv("L2_SYNC_LOG_DIR", str(tmp_path))
    logger = configure_logging(module_name="loader-l2-m1")

    try:
        file_names = [
            Path(cast(Any, handler).baseFilename).name
            for handler in logger.handlers
            if hasattr(handler, "baseFilename")
        ]

        assert logger.name == "crypto_l2_loader.loader-l2-m1"
        assert "loader-l2-m1.log" in file_names
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logging.getLogger("crypto_l2_loader.loader-l2-m1").handlers.clear()


def test_load_env_file_populates_missing_environment_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Local config files should provide process environment defaults."""

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "L2_INGEST_SYMBOLS=BTC ETH",
                "L2_INGEST_LEVELS=50",
                "EXISTING_VALUE=from_file",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("L2_INGEST_SYMBOLS", raising=False)
    monkeypatch.setenv("EXISTING_VALUE", "from_process")

    load_env_file(str(env_file))

    assert env_list("L2_INGEST_SYMBOLS", []) == ["BTC", "ETH"]
    assert env_list("MISSING_LIST", ["BTC"]) == ["BTC"]
    assert env_list("EXISTING_VALUE", []) == ["from_process"]
