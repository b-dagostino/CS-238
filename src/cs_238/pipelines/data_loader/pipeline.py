"""
This is a boilerplate pipeline 'data_loader'
generated using Kedro 1.0.0
"""

from typing import Any

from kedro.pipeline import Node, Pipeline

from .nodes import clean


def create_pipeline(**kwargs: Any) -> Pipeline:
    return Pipeline(
        [
            Node(
                func=clean,
                inputs=["raw_alarms", "params:data_loader.alarms"],
                outputs="clean_alarms",
                name="clean_alarms",
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
                outputs="clean_station_overviews",
                name="clean_station_overviews",
            ),
        ]
    )
