# Copyright (c) 2024 Snowflake Inc.

from utils.sql_utils import call_procedure, variant_argument


def finalize_connector_configuration(org_name: str, repo_name: str):
    config = {"org_name": org_name, "repo_name": repo_name}
    return call_procedure(
        "PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION",
        [variant_argument(config)]
    )
