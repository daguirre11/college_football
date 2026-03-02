import json
from pathlib import Path
from typing import (
    Sequence,
    Union,
)

import polars as pl


def _load_json_into_pl_df(*, json_path: Path) -> pl.DataFrame:
    return pl.DataFrame(json.loads(json_path.read_text()))


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
    cols_to_unnest: Union[Sequence[str], None] = None,
) -> pl.DataFrame:
    df = _load_json_into_pl_df(json_path=json_path)
    if cols_to_explode:
        for col_to_explode in cols_to_explode:
            df = df.explode(col_to_explode)

    if cols_to_unnest:
        for col_to_unnest in cols_to_unnest:
            df = df.with_columns(
                pl.col(col_to_unnest).struct.rename_fields(
                    [
                        "venue_id" if field == "id" else field
                        for field in df[col_to_unnest].struct.fields
                    ]
                )
            ).unnest(col_to_unnest)
    return _remove_unecessary_cols(df=df, cols_to_remove=cols_to_remove)
