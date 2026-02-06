from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_folder_path() -> Path:
    return Path(__file__).parent.resolve()


@pytest.fixture(scope="session")
def test_fixture_path(test_folder_path: Path) -> Path:
    return Path(test_folder_path / "fixtures")
