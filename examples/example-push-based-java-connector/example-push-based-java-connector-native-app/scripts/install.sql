SET APP_NAME = '&{APP_NAME}';
SET APP_INSTANCE_NAME = $APP_NAME || '_INSTANCE';

USE ROLE ACCOUNTADMIN;

DROP APPLICATION IF EXISTS IDENTIFIER($APP_INSTANCE_NAME) CASCADE;
CREATE APPLICATION IDENTIFIER($APP_INSTANCE_NAME) FROM APPLICATION PACKAGE IDENTIFIER($APP_NAME) USING VERSION &APP_VERSION;
