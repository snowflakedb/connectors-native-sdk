-- Copyright (c) 2024 Snowflake Inc.
SET APP_NAME = '&{APP_NAME}';
SET WAREHOUSE = '&{WAREHOUSE}';

SET APP_INSTANCE_NAME = $APP_NAME || '_INSTANCE';
SET SECRETS_DB = $APP_NAME || '_SECRETS';
SET SECRETS_SCHEMA = $SECRETS_DB || '.public';
SET SECRET_NAME = $SECRETS_DB || '.public.github_token';

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE IDENTIFIER($SECRETS_DB);
USE DATABASE IDENTIFIER($SECRETS_DB);

CREATE OR REPLACE NETWORK RULE GH_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST=('api.github.com:443');

CREATE OR REPLACE SECRET IDENTIFIER($SECRET_NAME) TYPE=GENERIC_STRING SECRET_STRING='&{token}';

set CREATE_INTEGRATION = 'CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_INTEGRATION
ALLOWED_NETWORK_RULES = (GH_RULE)
ALLOWED_AUTHENTICATION_SECRETS = (''' || $SECRET_NAME || ''')
ENABLED = TRUE';

select $CREATE_INTEGRATION;
execute immediate $CREATE_INTEGRATION;


-- Dev: install the app
DROP APPLICATION IF EXISTS IDENTIFIER($APP_INSTANCE_NAME) CASCADE;
CREATE APPLICATION IDENTIFIER($APP_INSTANCE_NAME) FROM APPLICATION PACKAGE IDENTIFIER($APP_NAME) USING VERSION &APP_VERSION;


GRANT USAGE ON INTEGRATION GITHUB_INTEGRATION TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);

GRANT USAGE ON DATABASE IDENTIFIER($SECRETS_DB) TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);
GRANT USAGE ON SCHEMA IDENTIFIER($SECRETS_SCHEMA)  TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);
GRANT READ ON SECRET IDENTIFIER($SECRET_NAME) TO APPLICATION IDENTIFIER($APP_INSTANCE_NAME);
