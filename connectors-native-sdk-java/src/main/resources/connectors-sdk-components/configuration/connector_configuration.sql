-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE PROCEDURE PUBLIC.CONFIGURE_CONNECTOR(config VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('@snowpark@')
    IMPORTS = ('@java_jar@')
    HANDLER = 'com.snowflake.connectors.application.configuration.connector.ConfigureConnectorHandler.configureConnector';
GRANT USAGE ON PROCEDURE PUBLIC.CONFIGURE_CONNECTOR(VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.CONFIGURE_CONNECTOR_INTERNAL(config VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.CONFIGURE_CONNECTOR_VALIDATE(config VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;
