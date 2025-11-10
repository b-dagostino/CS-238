"""
This is a boilerplate pipeline 'value_mapper'
generated using Kedro 1.0.0
"""

import logging
from typing import Final

import pandas as pd

_LOGGER: Final = logging.getLogger(__name__)


def discrete_category_mapper(df: pd.DataFrame, column: str) -> pd.DataFrame:
    unique_categories = df[column].dropna().unique()
    category_to_int = {category: idx + 1 for idx, category in enumerate(unique_categories)}
    return pd.Series(category_to_int, name="id").to_frame()


def map_serial_numbers(*dfs) -> pd.DataFrame:
    _LOGGER.info("Mapping serial numbers from %d dataframes", len(dfs))
    sns = []
    for df in dfs:
        sns.append(df["System S/N"])
    _LOGGER.info("Concatenating %d serial number series", len(sns))
    sns = pd.concat(sns).dropna().drop_duplicates().sort_values().reset_index(drop=True)
    return discrete_category_mapper(sns[~sns.str.contains(r"\.0")].to_frame(), column="System S/N")


def map_timesteps(*dfs) -> pd.DataFrame:
    _LOGGER.info("Mapping timesteps from %d dataframes", len(dfs))
    timestamps = []
    for df in dfs:
        timestamps.append(df["Data Timestamp"])
    _LOGGER.info("Concatenating %d timestamp series", len(timestamps))
    timestamps = pd.concat(timestamps).dropna().drop_duplicates().sort_values().reset_index(drop=True)
    return discrete_category_mapper(timestamps.to_frame(), column="Data Timestamp")
