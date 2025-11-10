"""
This is a boilerplate pipeline 'build_domain_model'
generated using Kedro 1.0.0
"""

import pandas as pd


def domain_alarms(alarms: pd.DataFrame, sn_map: pd.DataFrame, a_map: pd.DataFrame) -> pd.DataFrame:
    x = alarms.drop(columns=["Alarm Time", "Port"]).rename(columns={"Data Timestamp": "timestamp"})
    x = x.join(sn_map, on="System S/N", how="inner").rename(columns={"id": "cid"}).drop(columns=["System S/N"])
    x = x.join(a_map, on="Alarm Name", how="inner").rename(columns={"id": "aid"}).drop(columns=["Alarm Name"])
    x = x.sort_values(by=["cid", "timestamp"], ignore_index=True)
    x = x.set_index(["cid", "timestamp"])
    return x


def domain_status(
    status: pd.DataFrame, sn_map: pd.DataFrame, ss_map: pd.DataFrame, ns_map: pd.DataFrame
) -> pd.DataFrame:
    x = status[["System S/N", "Data Timestamp", "Station Status", "Network Status"]]
    x = x.rename(columns={"Data Timestamp": "timestamp"})
    x = x.join(sn_map, on="System S/N", how="inner").rename(columns={"id": "cid"}).drop(columns=["System S/N"])
    x = x.join(ss_map, on="Station Status", how="inner").rename(columns={"id": "ssid"}).drop(columns=["Station Status"])
    x = x.join(ns_map, on="Network Status", how="inner").rename(columns={"id": "nsid"}).drop(columns=["Network Status"])
    x = x.sort_values(by=["cid", "timestamp"], ignore_index=True)
    x = x.set_index(["cid", "timestamp"])
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
