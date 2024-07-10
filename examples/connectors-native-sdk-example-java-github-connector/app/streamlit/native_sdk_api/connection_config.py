# Copyright (c) 2024 Snowflake Inc.

from utils.sql_utils import call_procedure, empty_variant_argument


def set_connection_configuration():
    return call_procedure(
        "PUBLIC.SET_CONNECTION_CONFIGURATION",
        [empty_variant_argument()]
    )
