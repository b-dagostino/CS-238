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


def fix_station_overview(df: pd.DataFrame) -> pd.DataFrame:
    # Remove trailing .0 from System S/N values
    df["System S/N"] = df["System S/N"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Drop "nan" string entries in System S/N column
    df = df[df["System S/N"] != "nan"].reset_index(drop=True)
    return df


def prune_alarms(alarms_df: pd.DataFrame, station_overviews_df: pd.DataFrame) -> pd.DataFrame:
    """Prune all alarms before earliest station overview."""
    return alarms_df[alarms_df["Data Timestamp"] >= station_overviews_df["Data Timestamp"].min()]
