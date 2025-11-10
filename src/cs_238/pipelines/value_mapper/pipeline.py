"""
This is a boilerplate pipeline 'value_mapper'
generated using Kedro 1.0.0
"""

from functools import partial

from kedro.pipeline import Node, Pipeline  # noqa

from .nodes import discrete_category_mapper, map_serial_numbers, map_timesteps


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                func=partial(discrete_category_mapper, column="Alarm Name"),
                inputs=["clean_alarms"],
                outputs="map_alarm",
                name="map_alarms",
            ),
            Node(
                func=partial(discrete_category_mapper, column="Network Status"),
                inputs=["clean_station_overviews"],
                outputs="map_network_status",
                name="map_network_status",
            ),
            Node(
                func=partial(discrete_category_mapper, column="Station Status"),
                inputs=["clean_station_overviews"],
                outputs="map_station_status",
                name="map_station_status",
            ),
            # Node(
            #     func=map_timesteps,
            #     inputs=["clean_alarms", "clean_station_overviews"],
            #     outputs="timesteps_map",
            #     name="map_timesteps",
            # ),
            Node(
                func=map_serial_numbers,
                inputs=["clean_alarms", "clean_station_overviews"],
                outputs="map_serial_number",
                name="map_serial_number",
            ),
        ]
    )
