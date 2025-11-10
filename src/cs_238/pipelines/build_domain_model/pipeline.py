"""
This is a boilerplate pipeline 'build_domain_model'
generated using Kedro 1.0.0
"""

from kedro.pipeline import Node, Pipeline  # noqa

from .nodes import domain_alarms, domain_status, domain_sessions


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                func=domain_alarms,
                inputs=["clean_alarms", "map_serial_number", "map_alarm"],
                outputs="domain_alarms",
                name="domain_alarms",
            ),
            Node(
                func=domain_status,
                inputs=["clean_station_overviews", "map_serial_number", "map_station_status", "map_network_status"],
                outputs="domain_status",
                name="domain_status",
            ),
            Node(
                func=domain_sessions,
                inputs=["clean_charging_sessions", "map_serial_number"],
                outputs="domain_sessions",
                name="domain_sessions",
            ),
        ]
    )
