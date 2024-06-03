# Copyright (c) 2024 Snowflake Inc.

from utils.sf_utils import escape_identifier
from utils.sql_utils import call_procedure, variant_argument
from native_sdk_api.connector_config_view import get_configuration


def set_connection_configuration(external_access_integration: str, secret: str):
    config = {
        "external_access_integration": escape_identifier(external_access_integration),
        "secret": escape_identifier(secret)
    }

    return call_procedure(
        "PUBLIC.SET_CONNECTION_CONFIGURATION",
        [variant_argument(config)]
    )


def get_connection_configuration():
    return get_configuration("connection_configuration")
