"""
This is a boilerplate pipeline 'data_loader'
generated using Kedro 1.0.0
"""

from typing import Any

from kedro.pipeline import Node, Pipeline

from .nodes import clean, fix_station_overview, prune_alarms


def create_pipeline(**kwargs: Any) -> Pipeline:
    return Pipeline(
        [
            Node(
                func=clean,
                inputs=["raw_alarms", "params:data_loader.alarms"],
                outputs="preprune_alarms",
                name="clean_alarms",
            ),
            Node(
                func=prune_alarms,
                inputs=["preprune_alarms", "clean_station_overviews"],
                outputs="clean_alarms",
                name="prune_alarms",
            ),
            Node(
                func=clean,
                inputs=["raw_charging_sessions", "params:data_loader.charging_sessions"],
                outputs="clean_charging_sessions",
                name="clean_charging_sessions",
            ),
            Node(
                func=clean,
                inputs=["raw_station_overviews", "params:data_loader.station_overviews"],
                outputs="temp_clean_station_overviews",
                name="clean_station_overviews",
            ),
            Node(
                func=fix_station_overview,
                inputs="temp_clean_station_overviews",
                outputs="clean_station_overviews",
                name="fixup_station_overview",
            ),
        ]
    )
