from typing import Dict

from snowflake.snowpark.functions import column
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    StructType,
    VariantType,
    StructField,
    StringType,
    IntegerType,
)
import _snowflake
from snowflake_github_connector.common import (
    ResourceConfigTable,
    TASKS_SCHEMA,
    AppConfigTable,
    AppStateTable,
    APP_ROLE,
    escape_name,
    sink_table_name,
)
from snowflake_github_connector.ingestion import fetch_single_batch, create_sink_table_and_view
import logging

logger = logging.getLogger(__name__)


def provision_connector(session: Session, config: Dict) -> str:
    destination_database = config["destination_database"]
    secret_name = config["secret_name"]
    external_access_integration_name = config["external_access_integration_name"]

    # Enable external access in procedures
    statements = [
        f"ALTER PROCEDURE PUBLIC.INGEST_DATA(STRING) SET "
        f"SECRETS = ('token' = {secret_name}) "
        f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_name})",
        f"CREATE DATABASE {destination_database}",
        f"GRANT USAGE ON DATABASE {destination_database} TO APPLICATION ROLE {APP_ROLE}",
        f"GRANT USAGE ON SCHEMA {destination_database}.PUBLIC TO APPLICATION ROLE {APP_ROLE}",
    ]

    for statement in statements:
        session.sql(statement).collect()

    resource_table = AppConfigTable.with_session(session)
    resource_table.merge("config", config)

    logger.info("Connector provisioned")
    return "Connector provisioned"


def enable_resource(session: Session, resource_id: str) -> str:
    app_config = AppConfigTable.with_session(session).get_value("config")
    destination_db = app_config["destination_database"]

    resource_table = ResourceConfigTable.with_session(session)
    resource_table.merge(resource_id, {"enabled": True})

    create_sink_table_and_view(destination_db, resource_id, session)

    # Create ingestion task
    task_name = f"{TASKS_SCHEMA}.INGEST_{escape_name(resource_id).upper()}"
    strategy = [
        f"CREATE OR REPLACE TASK IDENTIFIER('{task_name}')"
        "SCHEDULE = '30 minutes' "
        f"WAREHOUSE = reference('warehouse_reference') "
        f"AS CALL PUBLIC.INGEST_DATA('{resource_id}')",
        f"ALTER TASK IDENTIFIER('{task_name}') RESUME",
        f"EXECUTE TASK IDENTIFIER('{task_name}')",
    ]
    for cmd in strategy:
        session.sql(cmd).collect()
    logger.info("Enabled: %s", resource_id)
    return f"{resource_id} enabled"


def ingest_data(session: Session, resource_id: str) -> Dict:
    logger.info("Starting ingestion for %s", resource_id)
    state_table = AppStateTable.with_session(session)
    app_config = AppConfigTable.with_session(session)

    destination_db = app_config.get_value("config").get("destination_database")
    if not destination_db:
        msg = "Missing destination database in configuration"
        state_table.merge(resource_id, {"state": "FAILED", "reason": msg})
        logger.error(msg)
        raise Exception(msg)

    try:
        github_token = _snowflake.get_generic_secret_string("token")
        next_page_link = None
        while True:
            ingestion_state = fetch_single_batch(
                session,
                destination_db=destination_db,
                resource_name=resource_id,
                page_link=next_page_link,
                github_token=github_token,
            )
            state_table.merge(resource_id, {"state": "RUNNING", "ingestion": ingestion_state})

            logger.info("Batch fetched for %s", resource_id)
            next_page_link = ingestion_state.get("extras", {}).get("next_page_link")
            if next_page_link is None:
                break

        state_table.merge(resource_id, {"state": "DONE"})
        logger.info("Ingestion done for %s", resource_id)
        return ingestion_state
    except Exception as err:
        logger.error("Ingestion failed for %s, Reason: %s", resource_id, err)
        state_table.merge(resource_id, {"state": "FAILED", "reason": str(err)})
        raise err
