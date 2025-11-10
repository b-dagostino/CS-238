"""
This is a boilerplate pipeline 'data_loader'
generated using Kedro 1.0.0
"""

import logging
from collections.abc import Mapping, Sequence
from typing import Final

import pandas as pd
from pydantic import BaseModel, Field

_LOGGER: Final = logging.getLogger(__name__)


class CleanParams(BaseModel):
    dropna: bool = True
    reset_index: bool = True
    drop_columns: Sequence[str] = Field(default_factory=list)
    column_dtypes: Mapping[str, str] = Field(default_factory=dict)
    datetime_columns: Sequence[str] = Field(default_factory=list)


def clean(raw: pd.DataFrame, clean_params: Mapping) -> pd.DataFrame:
    params = CleanParams(**clean_params)
    cleaned = raw

    if params.dropna:
        _LOGGER.debug("Dropping all-null columns")
        cleaned = cleaned.dropna(axis=1, how="all")

    if params.drop_columns:
        _LOGGER.debug("Dropping columns: %s", params.drop_columns)
        cleaned = cleaned.drop(
            params.drop_columns,
            axis=1,
        )

    if params.column_dtypes:
        _LOGGER.debug("Casting columns to dtypes: %s", params.column_dtypes)
        cleaned.astype(params.column_dtypes)

    for col in params.datetime_columns:
        _LOGGER.debug("Parsing datetime column: %s", col)
        cleaned[col] = pd.to_datetime(cleaned[col])

    if params.reset_index:
        _LOGGER.debug("Resetting index")
        cleaned = cleaned.reset_index(drop=True)

    return cleaned


def discrete_category_mapper(df: pd.DataFrame, column: str) -> pd.DataFrame:
    unique_categories = df[column].dropna().unique()
    category_to_int = {category: idx + 1 for idx, category in enumerate(unique_categories)}
    return pd.Series(category_to_int).to_frame()
