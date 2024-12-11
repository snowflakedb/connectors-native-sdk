-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('@snowpark@')
    IMPORTS = ('@java_jar@')
    HANDLER = 'com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationHandler.setConnectionConfiguration';
GRANT USAGE ON PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION(VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION_INTERNAL(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION_VALIDATE(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.GET_CONNECTION_CONFIGURATION()
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    DECLARE
        connection_configuration VARIANT;
        cur CURSOR FOR SELECT value FROM STATE.APP_CONFIG WHERE KEY = 'connection_configuration';
    BEGIN
        OPEN cur;
        FETCH cur INTO connection_configuration;
        CLOSE cur;

        IF (connection_configuration IS NULL) THEN
            RETURN OBJECT_CONSTRUCT(
                'response_code', 'CONNECTION_CONFIGURATION_NOT_FOUND',
                'message', 'Connection configuration data does not exist in the database.'
            );
        ELSE
            RETURN OBJECT_INSERT(connection_configuration, 'response_code', 'OK');
        END IF;
    END;
GRANT USAGE ON PROCEDURE PUBLIC.GET_CONNECTION_CONFIGURATION() TO APPLICATION ROLE ADMIN;
GRANT USAGE ON PROCEDURE PUBLIC.GET_CONNECTION_CONFIGURATION() TO APPLICATION ROLE VIEWER;
