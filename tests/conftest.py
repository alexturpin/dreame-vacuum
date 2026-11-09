"""Load the checked-out custom integration through Home Assistant's loader."""

from pathlib import Path

import pytest


@pytest.fixture
def hass_config_dir(tmp_path: Path) -> str:
    (tmp_path / "custom_components").symlink_to(
        Path(__file__).resolve().parents[1] / "custom_components",
        target_is_directory=True,
    )
    return str(tmp_path)
