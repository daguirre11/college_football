from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from college_football.schemas.common import (
    PolarsField,
    PolarsSchema,
)


def test_valid_field() -> None:
    field = PolarsField(name="school", dtype=pl.String)
    assert field.name == "school"
    assert field.dtype == pl.String
    assert field.nullable is True


def test_nullable_false() -> None:
    field = PolarsField(name="id", dtype=pl.Int64, nullable=False)
    assert field.nullable is False


def test_name_strips_whitespace() -> None:
    field = PolarsField(name="  school  ", dtype=pl.String)
    assert field.name == "school"


def test_empty_name_raises() -> None:
    with pytest.raises(ValidationError, match="Field name must not be empty"):
        PolarsField(name="", dtype=pl.String)


def test_whitespace_only_name_raises() -> None:
    with pytest.raises(ValidationError, match="Field name must not be empty"):
        PolarsField(name=" ", dtype=pl.String)


@pytest.mark.parametrize(
    ("input_name", "expected_name"),
    [
        ("camelCase", "camel_case"),
        ("PascalCase", "pascal_case"),
        ("alternateColor", "alternate_color"),
        ("already_snake", "already_snake"),
    ],
)
def test_name_converted_to_snake_case(
    input_name: str,
    expected_name: str,
) -> None:
    field = PolarsField(name=input_name, dtype=pl.String)
    assert field.name == expected_name


def test_invalid_dtype_raises() -> None:
    with pytest.raises(ValidationError, match="dtype must be a Polars DataType"):
        PolarsField(name="school", dtype="not_a_dtype")


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int64,
        pl.Float64,
        pl.String,
        pl.Boolean,
        pl.Date,
        pl.Datetime,
        pl.Categorical,
    ],
)
def test_valid_polars_dtypes(dtype: pl.DataType) -> None:
    field = PolarsField(name="col", dtype=dtype)
    assert field.dtype == dtype


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [2005, 2006, 333],
            "school": ["Air Force", "Akron", "Alabama"],
            "rating": [4.5, None, 3.2],
            "active": [True, False, True],
        }
    )


@pytest.fixture
def basic_schema() -> PolarsSchema:
    return PolarsSchema(
        fields=[
            PolarsField(name="id", dtype=pl.Int64, nullable=False),
            PolarsField(name="school", dtype=pl.String),
            PolarsField(name="rating", dtype=pl.Float64),
            PolarsField(name="active", dtype=pl.Boolean),
        ]
    )


def test_valid_schema(basic_schema: PolarsSchema) -> None:
    assert len(basic_schema.fields) == 4


def test_duplicate_field_names_raises() -> None:
    with pytest.raises(ValidationError, match="Duplicate field names found"):
        PolarsSchema(
            fields=[
                PolarsField(name="school", dtype=pl.String),
                PolarsField(name="school", dtype=pl.Int64),
            ]
        )


def test_to_schema(basic_schema: PolarsSchema) -> None:
    schema = basic_schema.to_schema()
    assert schema == {
        "id": pl.Int64,
        "school": pl.String,
        "rating": pl.Float64,
        "active": pl.Boolean,
    }


def test_apply_casts_columns(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="id", dtype=pl.Int32),
            PolarsField(name="school", dtype=pl.Categorical),
        ]
    )
    result = schema.apply(sample_df)
    assert result["id"].dtype == pl.Int32
    assert result["school"].dtype == pl.Categorical


def test_apply_ignores_missing_columns(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="nonexistent_col", dtype=pl.Int64),
        ]
    )
    result = schema.apply(sample_df)
    assert result.columns == sample_df.columns


def test_validate_df_passes(
    basic_schema: PolarsSchema,
    sample_df: pl.DataFrame,
) -> None:
    basic_schema.validate_df(sample_df)


def test_validate_df_wrong_dtype_raises(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="id", dtype=pl.String),  # wrong: actually Int64
        ]
    )
    with pytest.raises(ValueError, match="expected String, got Int64"):
        schema.validate_df(sample_df)


def test_validate_df_missing_column_raises(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="nonexistent", dtype=pl.String),
        ]
    )
    with pytest.raises(ValueError, match="missing column: 'nonexistent'"):
        schema.validate_df(sample_df)


def test_validate_df_non_nullable_with_nulls_raises(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="rating", dtype=pl.Float64, nullable=False),
        ]
    )
    with pytest.raises(ValueError, match="non-nullable but has 1 null"):
        schema.validate_df(sample_df)


def test_validate_df_non_nullable_without_nulls_passes(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="id", dtype=pl.Int64, nullable=False),
        ]
    )
    schema.validate_df(sample_df)


def test_check_missing_fields(
    basic_schema: PolarsSchema,
    sample_df: pl.DataFrame,
) -> None:
    assert basic_schema.check_missing_fields(sample_df) == []


def test_check_missing_fields_returns_missing(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="school", dtype=pl.String),
            PolarsField(name="missing_col", dtype=pl.Int64),
        ]
    )
    assert schema.check_missing_fields(sample_df) == ["missing_col"]


def test_check_extra_fields_none(
    basic_schema: PolarsSchema,
    sample_df: pl.DataFrame,
) -> None:
    assert basic_schema.check_extra_fields(sample_df) == []


def test_check_extra_fields_returns_extras(sample_df: pl.DataFrame) -> None:
    schema = PolarsSchema(
        fields=[
            PolarsField(name="id", dtype=pl.Int64),
        ]
    )
    extras = schema.check_extra_fields(sample_df)
    assert set(extras) == {"school", "rating", "active"}
