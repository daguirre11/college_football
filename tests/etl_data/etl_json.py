from pathlib import Path
from typing import (
    Sequence,
    Union,
)

import polars as pl
import pytest

from college_football.etl_data.etl_json import (
    _load_json_into_pl_df,
    _remove_unecessary_cols,
    load_football_json_as_df,
)

_sub_selection_cols = ["primary_id", "school", "logos"]
_sub_selection_dict = {
    "primary_id": [2005, 2006, 333],
    "school": ["Air Force", "Akron", "Alabama"],
    "logos": [
        [
            "http://a.espncdn.com/i/teamlogos/ncaa/500/2005.png",
            "http://a.espncdn.com/i/teamlogos/ncaa/500-dark/2005.png",
        ],
        [
            "http://a.espncdn.com/i/teamlogos/ncaa/500/2006.png",
            "http://a.espncdn.com/i/teamlogos/ncaa/500-dark/2006.png",
        ],
        [
            "http://a.espncdn.com/i/teamlogos/ncaa/500/333.png",
            "http://a.espncdn.com/i/teamlogos/ncaa/500-dark/333.png",
        ],
    ],
}


@pytest.fixture(scope="module")
def teams_data_small_df(test_fixture_path: Path) -> pl.DataFrame:
    return _load_json_into_pl_df(
        json_path=test_fixture_path / "2025_teams_data_small.json",
    )


def test_load_json(teams_data_small_df: pl.DataFrame) -> None:
    assert teams_data_small_df.height == 3
    assert (
        teams_data_small_df.select(
            _sub_selection_cols,
        ).to_dict(as_series=False)
        == _sub_selection_dict
    )


@pytest.mark.parametrize("cols_to_remove", [(["location", "twitter"]), (None)])
def test_remove_unecessary_cols(
    teams_data_small_df: pl.DataFrame, cols_to_remove: Union[Sequence[str], None]
) -> None:
    result = _remove_unecessary_cols(
        df=teams_data_small_df,
        cols_to_remove=cols_to_remove,
    )
    assert result.height == 3
    assert (
        result.select(
            _sub_selection_cols,
        ).to_dict(as_series=False)
        == _sub_selection_dict
    )
    if cols_to_remove:
        for col in result.columns:
            assert col not in cols_to_remove


@pytest.mark.parametrize("cols_to_explode", [(["location"]), (None)])
def test_get_json_as_df(
    cols_to_explode: Union[Sequence[str], None],
    test_fixture_path: Path,
) -> None:
    result = load_football_json_as_df(
        json_path=test_fixture_path / "2025_teams_data_small.json",
        cols_to_explode=cols_to_explode,
    )
    assert result.height == 3
    assert (
        result.select(
            _sub_selection_cols,
        ).to_dict(as_series=False)
        == _sub_selection_dict
    )
    if cols_to_explode:
        assert result.select(
            ["id", "state", "zip"],
        ).to_dict(as_series=False) == {
            "id": [3713, 3768, 3657],
            "state": ["CO", "OH", "AL"],
            "zip": ["80840", "44399", "35487"],
        }
