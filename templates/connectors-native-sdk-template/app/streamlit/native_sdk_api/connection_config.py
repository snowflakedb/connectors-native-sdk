# Copyright (c) 2024 Snowflake Inc.

from utils.sf_utils import escape_identifier
from utils.sql_utils import call_procedure, variant_argument
from native_sdk_api.connector_config_view import get_configuration


def set_connection_configuration(custom_connection_property: str):
    # TODO: this part of the code sends the config to the backend so all custom properties need to be added here
    config = {
        "custom_connection_property": escape_identifier(custom_connection_property),
    }

    return call_procedure(
        "PUBLIC.SET_CONNECTION_CONFIGURATION",
        [variant_argument(config)]
    )


def get_connection_configuration():
    return get_configuration("connection_configuration")
