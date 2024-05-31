# Copyright (c) 2024 Snowflake Inc.

from snowflake.snowpark.context import get_active_session


def get_configuration(config_group: str):
    config_rows = (
        get_active_session()
        .sql(f"SELECT config_key, value FROM PUBLIC.CONNECTOR_CONFIGURATION WHERE "
             f"config_group = '{config_group}'")
        .collect()
    )

    config = {}
    for row in config_rows:
        config[row[0]] = row[1]

    return config
