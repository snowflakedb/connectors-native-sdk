# Copyright (c) 2024 Snowflake Inc.

from utils.sql_utils import call_procedure, variant_argument


def finalize_connector_configuration(custom_property: str):
    # TODO: If some custom properties were configured, then they need to be specified here and passed to the FINALIZE_CONNECTOR_CONFIGURATION procedure.
    config = {"custom_property": custom_property}
    return call_procedure(
        "PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION",
        [variant_argument(config)]
    )
