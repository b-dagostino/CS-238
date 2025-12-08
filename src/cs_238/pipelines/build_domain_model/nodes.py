"""
This is a boilerplate pipeline 'build_domain_model'
generated using Kedro 1.0.0
"""

import logging
from collections.abc import Mapping, Sequence
from typing import Final

import pandas as pd

_LOGGER: Final = logging.getLogger(__name__)


def discrete_category_mapper(df: pd.DataFrame, column: str) -> pd.DataFrame:
    unique_categories = df[column].dropna().sort_values().unique()
    category_to_int = {category: idx + 1 for idx, category in enumerate(unique_categories)}
    return pd.Series(category_to_int, name="id").to_frame()


def map_serial_numbers(*dfs) -> pd.DataFrame:
    _LOGGER.info("Mapping serial numbers from %d dataframes", len(dfs))
    sns = []
    for df in dfs:
        sns.append(df["System S/N"])
    _LOGGER.info("Concatenating %d serial number series", len(sns))
    sns = pd.concat(sns, ignore_index=True)
    return discrete_category_mapper(sns.to_frame(), column="System S/N")


def map_timesteps(*dfs) -> pd.DataFrame:
    _LOGGER.info("Mapping timesteps from %d dataframes", len(dfs))
    timestamps = []
    for df in dfs:
        timestamps.append(df["Data Timestamp"])
    _LOGGER.info("Concatenating %d timestamp series", len(timestamps))
    timestamps = pd.concat(timestamps).dropna().drop_duplicates().sort_values().reset_index(drop=True)
    return discrete_category_mapper(timestamps.to_frame(), column="Data Timestamp")


def value_replacer(df: pd.DataFrame, replacement_groups: Mapping[str, Sequence[str]], column: str) -> pd.DataFrame:
    df = df.copy()
    # Create replacement mapping
    replacement_mapping = {
        original_value: replacement_value
        for replacement_value, original_values in replacement_groups.items()
        for original_value in original_values
    }
    replacement_mapping = pd.Series(replacement_mapping)

    # Apply replacements only where mapping exists
    df[column] = df[column].replace(replacement_mapping)
    return df


def domain_alarms(alarms: pd.DataFrame, sn_map: pd.DataFrame) -> pd.DataFrame:
    x = alarms.drop(columns=["Alarm Time", "Port"]).rename(
        columns={"Data Timestamp": "timestamp", "Alarm Name": "alarm"}
    )
    x = x.join(sn_map, on="System S/N", how="inner").rename(columns={"id": "cid"}).drop(columns=["System S/N"])
    # x = x.join(a_map, on="Alarm Name", how="inner").rename(columns={"id": "aid"}).drop(columns=["Alarm Name"])
    x = x.sort_values(by=["cid", "timestamp"], ignore_index=True)
    # x = x.set_index(["cid", "timestamp"])
    x = x[["cid", "timestamp", "alarm"]]
    return x


def domain_status(status: pd.DataFrame, sn_map: pd.DataFrame) -> pd.DataFrame:
    x = status[["System S/N", "Data Timestamp", "Station Status", "Network Status"]]
    x = x.rename(
        columns={"Data Timestamp": "timestamp", "Station Status": "functional_state", "Network Status": "network_state"}
    )
    x = x.join(sn_map, on="System S/N", how="inner").rename(columns={"id": "cid"}).drop(columns=["System S/N"])
    # x = x.join(ss_map, on="Station Status", how="inner").rename(columns={"id": "ssid"}).drop(columns=["Station Status"])
    # x = x.join(ns_map, on="Network Status", how="inner").rename(columns={"id": "nsid"}).drop(columns=["Network Status"])
    x = x.sort_values(by=["cid", "timestamp"], ignore_index=True)
    # x = x.set_index(["cid", "timestamp"])

    # Create combined state column
    x["state"] = pd.Series(zip(x.functional_state, x.network_state)).astype(str)

    x = x[["cid", "timestamp", "functional_state", "network_state", "state"]]

    # x["state-code"] = x["state"].astype("category").cat.codes + 1  # Start codes from 1

    return x


def domain_sessions(sessions: pd.DataFrame, sn_map: pd.DataFrame) -> pd.DataFrame:
    x = sessions[
        ["System S/N", "Start Timestamp", "End Timestamp", "Total Duration (hh:mm:ss)", "Charging Time (hh:mm:ss)"]
    ]
    x = x.rename(
        columns={
            "Start Timestamp": "start-timestamp",
            "End Timestamp": "end-timestamp",
            "Total Duration (hh:mm:ss)": "total-duration",
            "Charging Time (hh:mm:ss)": "charging-duration",
        }
    )
    x["total-duration"] = pd.to_timedelta(x["total-duration"])
    x["charging-duration"] = pd.to_timedelta(x["charging-duration"])
    x = x.join(sn_map, on="System S/N", how="inner").rename(columns={"id": "cid"}).drop(columns=["System S/N"])
    x = x.sort_values(by=["cid", "start-timestamp"], ignore_index=True)
    x = x.set_index(["cid", "start-timestamp"])
    return x


def intermediate_domain_observations(status: pd.DataFrame, alarms: pd.DataFrame) -> pd.DataFrame:
    # Select and rename columns
    status = status[["cid", "timestamp", "state"]].rename(columns={"state": "observation"})
    alarms = alarms[["cid", "timestamp", "alarm"]].rename(columns={"alarm": "observation"})

    # Concatenate observations
    observations = pd.concat([status, alarms], ignore_index=True)
    observations = observations.sort_values(by=["cid", "timestamp"], ignore_index=True)
    return observations


def domain_observations(observations: pd.DataFrame) -> pd.DataFrame:
    # observations["observation"] = observations["observation"].astype("category")
    # observations["oidx"] = observations["observation"].cat.codes
    return observations
