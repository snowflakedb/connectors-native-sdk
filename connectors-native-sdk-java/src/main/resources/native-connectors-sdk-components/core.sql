-- Copyright (c) 2024 Snowflake Inc.

-- connectors-native-sdk <sdk_version>2.1.0</sdk_version>

CREATE APPLICATION ROLE IF NOT EXISTS ADMIN;
CREATE APPLICATION ROLE IF NOT EXISTS VIEWER;
CREATE APPLICATION ROLE IF NOT EXISTS DATA_READER;

CREATE OR ALTER VERSIONED SCHEMA PUBLIC;
GRANT USAGE ON SCHEMA PUBLIC TO APPLICATION ROLE ADMIN;
GRANT USAGE ON SCHEMA PUBLIC TO APPLICATION ROLE VIEWER;

-- STATE
CREATE SCHEMA IF NOT EXISTS STATE;
GRANT USAGE ON SCHEMA STATE TO APPLICATION ROLE ADMIN;

CREATE TABLE IF NOT EXISTS STATE.APP_STATE (
    KEY STRING NOT NULL,
    VALUE VARIANT NOT NULL,
    UPDATED_AT TIMESTAMP_NTZ
);

MERGE INTO STATE.APP_STATE AS dest
    USING (SELECT 'connector_status' AS key,
                  OBJECT_CONSTRUCT('status', 'CONFIGURING', 'configurationStatus', 'INSTALLED') AS value,
                  SYSDATE() AS updated_at
    ) AS src
    ON dest.key = src.key
    WHEN NOT MATCHED THEN INSERT VALUES (src.key, src.value, src.updated_at);

CREATE OR REPLACE PROCEDURE PUBLIC.GET_CONNECTOR_STATUS()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
        AS
        DECLARE
            status VARIANT;
            cur CURSOR FOR SELECT value FROM STATE.APP_STATE WHERE KEY = 'connector_status';
        BEGIN
            OPEN cur;
            FETCH cur INTO status;
            CLOSE cur;

            RETURN
                CASE
                    WHEN status IS NULL THEN
                        OBJECT_CONSTRUCT(
                                'response_code', 'CONNECTOR_STATUS_NOT_FOUND',
                                'message', 'Connector status data does not exist in the database'
                            )
                    WHEN status:status = 'PAUSING' THEN
                        OBJECT_INSERT(
                                OBJECT_INSERT(status, 'status', 'STARTED', TRUE),
                                'response_code', 'OK'
                            )
                    WHEN status:status = 'STARTING' THEN
                        OBJECT_INSERT(
                                OBJECT_INSERT(status, 'status', 'PAUSED', TRUE),
                                'response_code', 'OK'
                            )
                    ELSE
                        OBJECT_INSERT(status, 'response_code', 'OK')
                END;
        END;
GRANT USAGE ON PROCEDURE PUBLIC.GET_CONNECTOR_STATUS() TO APPLICATION ROLE ADMIN;
GRANT USAGE ON PROCEDURE PUBLIC.GET_CONNECTOR_STATUS() TO APPLICATION ROLE VIEWER;
