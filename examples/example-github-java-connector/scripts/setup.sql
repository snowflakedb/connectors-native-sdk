SET APP_NAME = '&{APP_NAME}';
SET WAREHOUSE = 'xs';

SET APP_INSTANCE_NAME = $APP_NAME || '_INSTANCE';
SET SECRETS_DB = $APP_NAME || '_SECRETS';
SET SECRETS_SCHEMA = $SECRETS_DB || '.public';
SET SECRET_NAME = $SECRETS_DB || '.public.github_token';
SET INTEGRATION_NAME = $APP_NAME || '_INTEGRATION';

SELECT $APP_NAME;
SELECT $APP_INSTANCE_NAME;
SELECT $SECRET_NAME;

use role accountadmin;

CREATE OR REPLACE DATABASE IDENTIFIER($SECRETS_DB);
use database IDENTIFIER($SECRETS_DB);

create or replace network rule gh_rule
mode = egress
type = host_port
value_list=('api.github.com:443');

create or replace secret IDENTIFIER($SECRET_NAME) type=generic_string secret_string='&{token}';

set CREATE_INTEGRATION = 'CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ' || $INTEGRATION_NAME || '
ALLOWED_NETWORK_RULES = (GH_RULE)
ALLOWED_AUTHENTICATION_SECRETS = (''' || $SECRET_NAME || ''')
ENABLED = TRUE';

select $CREATE_INTEGRATION;
execute immediate $CREATE_INTEGRATION;

grant usage on integration IDENTIFIER($INTEGRATION_NAME) to application identifier($APP_INSTANCE_NAME);

grant usage on database IDENTIFIER($SECRETS_DB) to application identifier($APP_INSTANCE_NAME);
grant usage on schema IDENTIFIER($SECRETS_SCHEMA)  to application identifier($APP_INSTANCE_NAME);
grant read on secret IDENTIFIER($SECRET_NAME) to application identifier($APP_INSTANCE_NAME);
