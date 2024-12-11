# Copyright (c) 2024 Snowflake Inc.

from utils.sf_utils import escape_identifier
from utils.sql_utils import call_procedure, variant_argument
from native_sdk_api.connector_config_view import get_configuration


def configure_connector(dest_db: str, dest_schema: str):
    config = {
        "destination_database": escape_identifier(dest_db),
        "destination_schema": escape_identifier(dest_schema),
        "global_schedule": {
            "scheduleType": "CRON",
            "scheduleDefinition": "*/1 * * * *"
        }
    }

    return call_procedure(
        "PUBLIC.CONFIGURE_CONNECTOR",
        [variant_argument(config)]
    )


def get_connector_configuration():
    return get_configuration("connector_configuration")
