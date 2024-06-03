# Copyright (c) 2024 Snowflake Inc.

from snowflake.snowpark.context import get_active_session
import pandas as pd


def get_aggregated_connector_stats():
    session = get_active_session()
    result = session.sql(
        "SELECT RUN_DATE, UPDATED_ROWS FROM PUBLIC.AGGREGATED_CONNECTOR_STATS "
        "WHERE RUN_DATE > DATEADD(DAY, -1, CURRENT_TIMESTAMP());"
    ).collect()
    return pd.DataFrame(result)
