-- Copyright (c) 2024 Snowflake Inc.

CREATE OR REPLACE PROCEDURE PUBLIC.RUN_SCHEDULER_ITERATION()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.14.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.application.scheduler.RunSchedulerIterationHandler.runIteration';

CREATE OR REPLACE PROCEDURE PUBLIC.CREATE_SCHEDULER()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.14.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.application.scheduler.CreateSchedulerHandler.createScheduler';
GRANT USAGE ON PROCEDURE PUBLIC.CREATE_SCHEDULER() TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.ON_INGESTION_SCHEDULED(process_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    END;
