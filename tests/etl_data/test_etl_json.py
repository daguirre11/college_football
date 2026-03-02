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

_sub_selection_cols = ["id", "school"]
_sub_selection_dict = {
    "id": [2005, 2006, 333],
    "school": ["Air Force", "Akron", "Alabama"],
}
_sub_selection_dict_explode = {
    "id": [2005, 2005, 2006, 2006, 333, 333],
    "school": ["Air Force", "Air Force", "Akron", "Akron", "Alabama", "Alabama"],
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


@pytest.mark.parametrize(
    (
        "cols_to_explode",
        "cols_to_unnest",
        "expected_amount_rows",
        "sub_selection_values",
    ),
    [
        (["alternateNames"], None, 6, _sub_selection_dict_explode),
        (None, ["location"], 3, _sub_selection_dict),
    ],
)
def test_get_json_as_df(
    cols_to_explode: Union[Sequence[str], None],
    cols_to_unnest: Union[Sequence[str], None],
    expected_amount_rows: int,
    sub_selection_values: dict[str, Union[int, str]],
    test_fixture_path: Path,
) -> None:
    result = load_football_json_as_df(
        json_path=test_fixture_path / "2025_teams_data_small.json",
        cols_to_explode=cols_to_explode,
        cols_to_unnest=cols_to_unnest,
    )
    assert result.height == expected_amount_rows
    assert (
        result.select(
            _sub_selection_cols,
        ).to_dict(as_series=False)
        == sub_selection_values
    )
    if cols_to_unnest:
        assert result.select(
            ["id", "state", "zip"],
        ).to_dict(as_series=False) == {
            "id": [2005, 2006, 333],
            "state": ["CO", "OH", "AL"],
            "zip": ["80840", "44399", "35487"],
        }
