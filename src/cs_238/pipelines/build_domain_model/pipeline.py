"""
This is a boilerplate pipeline 'build_domain_model'
generated using Kedro 1.0.0
"""

from functools import partial

from kedro.pipeline import Node, Pipeline  # noqa

from .nodes import (
    discrete_category_mapper,
    domain_alarms,
    domain_observations,
    domain_sessions,
    domain_status,
    intermediate_domain_observations,
    map_serial_numbers,
    value_replacer,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                func=map_serial_numbers,
                inputs=["clean_alarms", "clean_station_overviews"],
                outputs="map_serial_number",
                name="map_serial_number",
            ),
            # Node(
            #     func=partial(value_replacer, column="Alarm Name"),
            #     inputs=["clean_alarms", "params:value-replacer.alarms"],
            #     outputs="domain_alarms_intermediate",
            #     name="value_replacer_alarms",
            # ),
            # Node(
            #     func=partial(discrete_category_mapper, column="Alarm Name"),
            #     inputs=["clean_alarms"],
            #     outputs="map_alarm",
            #     name="map_alarms",
            # ),
            Node(
                func=domain_alarms,
                inputs=["clean_alarms", "map_serial_number"],
                outputs="domain_alarms",
                name="create_domain_alarms",
            ),
            Node(
                func=partial(value_replacer, column="Station Status"),
                inputs=["clean_station_overviews", "params:value-replacer.station-status"],
                outputs="temp_domain_intermediate_1_station_overviews",
                name="replace_station_status",
            ),
            Node(
                func=partial(value_replacer, column="Network Status"),
                inputs=["temp_domain_intermediate_1_station_overviews", "params:value-replacer.network-status"],
                outputs="temp_domain_intermediate_2_station_overviews",
                name="replace_network_status",
            ),
            # Node(
            #     func=partial(discrete_category_mapper, column="Network Status"),
            #     inputs=["domain_station_overviews_intermediate_2"],
            #     outputs="map_network_status",
            #     name="map_network_status",
            # ),
            # Node(
            #     func=partial(discrete_category_mapper, column="Station Status"),
            #     inputs=["domain_station_overviews_intermediate_2"],
            #     outputs="map_station_status",
            #     name="map_station_status",
            # ),
            Node(
                func=domain_status,
                inputs=[
                    "temp_domain_intermediate_2_station_overviews",
                    "map_serial_number",
                ],
                outputs="domain_status",
                name="create_domain_status",
            ),
            # Node(
            #     func=domain_sessions,
            #     inputs=["clean_charging_sessions", "map_serial_number"],
            #     outputs="domain_sessions",
            #     name="domain_sessions",
            # ),
            Node(
                func=intermediate_domain_observations,
                inputs=["domain_status", "domain_alarms"],
                outputs="temp_domain_intermediate_observations",
                name="create_intermediate_domain_observations",
            ),
            Node(
                func=domain_observations,
                inputs=["temp_domain_intermediate_observations"],
                outputs="domain_observations",
                name="create_domain_observations",
            ),
        ]
    )
