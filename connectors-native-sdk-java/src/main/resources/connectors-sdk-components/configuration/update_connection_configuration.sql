-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_CONNECTION_CONFIGURATION(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('@snowpark@')
    IMPORTS = ('@java_jar@')
    HANDLER = 'com.snowflake.connectors.application.configuration.connection.UpdateConnectionConfigurationHandler.updateConnectionConfiguration';
GRANT USAGE ON PROCEDURE PUBLIC.UPDATE_CONNECTION_CONFIGURATION(VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.DRAFT_CONNECTION_CONFIGURATION_INTERNAL(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
        AS
        BEGIN
            RETURN OBJECT_CONSTRUCT('response_code', 'OK');
        END;

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_CONNECTION_CONFIGURATION_VALIDATE(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
        AS
        BEGIN
            RETURN OBJECT_CONSTRUCT('response_code', 'OK');
        END;
