import json
from pathlib import Path
from typing import (
    Sequence,
    Union,
)

import polars as pl


def _load_json_into_pl_df(*, json_path: Path) -> pl.DataFrame:
    data = json.loads(json_path.read_text())
    return pl.DataFrame(data).rename({"id": "primary_id"})


def _remove_unecessary_cols(
    *,
    df: pl.DataFrame,
    cols_to_remove: Union[Sequence[str], None],
) -> pl.DataFrame:
    return df.drop(cols_to_remove) if cols_to_remove else df


def load_football_json_as_df(
    json_path: Path,
    cols_to_explode: Union[Sequence[str], None] = None,
    cols_to_remove: Union[Sequence[str], None] = None,
) -> pl.DataFrame:
    df = _load_json_into_pl_df(json_path=json_path)
    if cols_to_explode:
        for col_to_explode in cols_to_explode:
            df = df.unnest(col_to_explode)
    return _remove_unecessary_cols(df=df, cols_to_remove=cols_to_remove)
