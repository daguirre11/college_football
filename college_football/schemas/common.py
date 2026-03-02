from __future__ import annotations

import re
from typing import Any

import polars as pl
from pydantic import (
    BaseModel,
    field_validator,
    model_validator,
)


class PolarsField(BaseModel):
    name: str
    dtype: Any
    nullable: bool = True

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field name must not be empty.")
        return v.strip()

    @field_validator("name")
    @classmethod
    def name_to_snake_case(cls, v: str) -> str:
        v = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", v)
        v = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", v)
        return v.lower()

    @field_validator("dtype")
    @classmethod
    def dtype_must_be_polars_type(cls, v: Any) -> Any:
        if not isinstance(v, (pl.DataType, type)):
            raise ValueError(f"dtype must be a Polars DataType, got {type(v)}")
        return v


class PolarsSchema(BaseModel):
    fields: list[PolarsField]

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="after")
    def check_no_duplicate_field_names(self) -> PolarsSchema:
        names = [field.name for field in self.fields]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(f"Duplicate field names found: {duplicates}")
        return self

    def to_schema(self) -> dict[str, pl.DataType]:
        return {field.name: field.dtype for field in self.fields}

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        exprs = [
            pl.col(field.name).cast(field.dtype)
            for field in self.fields
            if field.name in df.columns
        ]
        return df.with_columns(exprs)

    def validate_df(self, df: pl.DataFrame) -> None:
        errors = []

        for field in self.fields:
            if field.name not in df.columns:
                errors.append(f"missing column: '{field.name}'")
                continue
            actual = df[field.name].dtype
            if actual != field.dtype:
                errors.append(f"  '{field.name}': expected {field.dtype}, got {actual}")
            if not field.nullable and df[field.name].null_count() > 0:
                errors.append(
                    f"  '{field.name}': non-nullable but has "
                    f"{df[field.name].null_count()} null(s)"
                )

        if errors:
            raise ValueError("Schema validation failed:\n" + "\n".join(errors))

    def check_missing_fields(self, df: pl.DataFrame) -> list[str]:
        return [field.name for field in self.fields if field.name not in df.columns]

    def check_extra_fields(self, df: pl.DataFrame) -> list[str]:
        schema_names = {field.name for field in self.fields}
        return [col for col in df.columns if col not in schema_names]
