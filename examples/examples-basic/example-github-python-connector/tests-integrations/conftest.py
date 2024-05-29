# Copyright (c) 2024 Snowflake Inc.
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from snowflake.snowpark import Session
from pytest import fixture


@dataclass
class Environment:
    app_name: str
    app_instance: str
    secret_name: str
    warehouse: str
    dest_db: str


@fixture(scope="session", autouse=True)
def build_package():
    subprocess.check_call(["python", "-m", "build"], cwd=Path(__file__).parent.parent)


@fixture(scope="session")
def snowpark_session():
    config_file = Path(__file__).parent / "sf_config.json"
    new_session = Session.builder.configs(json.loads(config_file.read_text())).create()
    yield new_session


@fixture(scope="session")
def environment(snowpark_session):
    random_prefix = os.environ.get("USER", "TEST") + "_" + str(int(time.time()))
    app_name = random_prefix + "_GITHUB_CONNECTOR"
    instance_name = app_name + "_instance"

    stage_db = random_prefix + "_stage_db"
    stage_name = f"{stage_db}.PUBLIC.CONNECTOR_STAGE"
    secrets_db = f"{app_name}_secrets"
    secret_name = f"{secrets_db}.public.github_token"
    version = "V_1_0_0"
    token = os.environ["GITHUB_TOKEN"]
    warehouse = "xs"
    sqls = [
        "USE ROLE ACCOUNTADMIN;",
        f"SET APP_NAME = '{app_name}';",
        f"SET APP_INSTANCE_NAME = '{instance_name}';",
        # deploy
        f"CREATE OR REPLACE DATABASE IDENTIFIER('{stage_db}');",
        f"CREATE STAGE IF NOT EXISTS IDENTIFIER('{stage_name}');",
        f"PUT 'file://sf_build/*' @{stage_name}/{version} AUTO_COMPRESS = FALSE overwrite=true;",
        "CREATE APPLICATION PACKAGE IF NOT EXISTS IDENTIFIER($APP_NAME);",
        # install
        f"CREATE APPLICATION IDENTIFIER($APP_INSTANCE_NAME) "
        f"FROM APPLICATION PACKAGE IDENTIFIER($APP_NAME) USING @{stage_name}/{version};",
        f"CREATE OR REPLACE DATABASE {secrets_db};",
        "CREATE OR REPLACE NETWORK RULE GH_RULE "
        "MODE = EGRESS "
        "TYPE = HOST_PORT "
        "VALUE_LIST=('api.github.com:443');",
        f"CREATE OR REPLACE SECRET {secret_name} TYPE=GENERIC_STRING SECRET_STRING='{token}';",
        "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_INTEGRATION "
        "ALLOWED_NETWORK_RULES = (GH_RULE) "
        f"ALLOWED_AUTHENTICATION_SECRETS = ('{secret_name}') "
        "ENABLED = TRUE;",
        "GRANT USAGE ON INTEGRATION GITHUB_INTEGRATION TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        f"GRANT USAGE ON DATABASE {secrets_db} TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        f"GRANT USAGE ON SCHEMA {secrets_db}.public  TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        f"GRANT READ ON SECRET {secret_name} TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        "GRANT CREATE DATABASE ON ACCOUNT TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        "GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        "GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        "GRANT USAGE ON DATABASE LOGS TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        "GRANT USAGE ON SCHEMA LOGS.PUBLIC TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        "GRANT SELECT ON TABLE LOGS.PUBLIC.EVENT_TABLE TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
        f"GRANT USAGE ON WAREHOUSE {warehouse} TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);",
    ]
    for sql in sqls:
        snowpark_session.sql(sql).collect()
    yield Environment(
        app_name=app_name,
        app_instance=instance_name,
        secret_name=secret_name,
        warehouse=warehouse,
        dest_db=f"DEST_{app_name}",
    )
