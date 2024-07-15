-- Copyright (c) 2024 Snowflake Inc.

-- CONNECTORS-NATIVE-SDK
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/all.sql';
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/task_reactor.sql';

-- CUSTOM CONNECTOR OBJECTS
CREATE OR ALTER VERSIONED SCHEMA STREAMLIT;
GRANT USAGE ON SCHEMA STREAMLIT TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE STREAMLIT STREAMLIT.NATIVE_SDK_TEMPLATE_ST
    FROM  '/streamlit'
    MAIN_FILE = 'streamlit_app.py';
GRANT USAGE ON STREAMLIT STREAMLIT.NATIVE_SDK_TEMPLATE_ST TO APPLICATION ROLE ADMIN;

-- SNOWFLAKE REFERENCE MECHANISM
CREATE OR REPLACE PROCEDURE PUBLIC.REGISTER_REFERENCE(ref_name STRING, operation STRING, ref_or_alias STRING)
    RETURNS STRING
    LANGUAGE SQL
    AS
        BEGIN
            CASE (operation)
                WHEN 'ADD' THEN
                    SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
                WHEN 'REMOVE' THEN
                    SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
                WHEN 'CLEAR' THEN
                    SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
                ELSE RETURN 'unknown operation: ' || operation;
                END CASE;
            RETURN NULL;
        END;
GRANT USAGE ON PROCEDURE PUBLIC.REGISTER_REFERENCE(STRING, STRING, STRING) TO APPLICATION ROLE ADMIN;

-----------------WIZARD-----------------
-- PREREQUISITES
MERGE INTO STATE.PREREQUISITES AS dest
    USING (SELECT * FROM VALUES
               ('1',
                'Sample prerequisite',
                'Prerequisites can be used to notice the end user of the connector about external configurations. Read more in the SDK documentation below. This content can be modified inside `setup.sql` script',
                'https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/prerequisites',
                NULL,
                NULL,
                1
               )
    ) AS src (id, title, description, documentation_url, learnmore_url, guide_url, position)
    ON dest.id = src.id
    WHEN NOT MATCHED THEN
        INSERT (id, title, description, documentation_url, learnmore_url, guide_url, position)
        VALUES (src.id, src.title, src.description, src.documentation_url, src.learnmore_url, src.guide_url, src.position);

-- CONNECTION CONFIGURATION
CREATE OR REPLACE PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-template.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.configuration.connection.TemplateConnectionConfigurationHandler.setConnectionConfiguration';
GRANT USAGE ON PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION(VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.TEST_CONNECTION()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-template.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.configuration.connection.TemplateConnectionValidator.testConnection';

-- FINALIZE CONFIGURATION
CREATE OR REPLACE PROCEDURE PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION(CUSTOM_CONFIGURATION VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-template.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.configuration.finalize.TemplateFinalizeConnectorConfigurationCustomHandler.finalizeConnectorConfiguration';
GRANT USAGE ON PROCEDURE PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION(VARIANT) TO APPLICATION ROLE ADMIN;

-----------------TASK REACTOR-----------------
CREATE OR REPLACE PROCEDURE PUBLIC.TEMPLATE_WORKER(worker_id number, task_reactor_schema string)
    RETURNS STRING
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0', 'com.snowflake:telemetry:latest')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.TemplateWorker.executeWork';

CALL TASK_REACTOR.CREATE_INSTANCE_OBJECTS(
        'EXAMPLE_CONNECTOR_TASK_REACTOR',
        'PUBLIC.TEMPLATE_WORKER',
        'VIEW',
        'EXAMPLE_CONNECTOR_TASK_REACTOR.WORK_SELECTOR_VIEW',
    -- TODO: Below NULL causes the application to create and use default EMPTY_EXPIRED_WORK_SELECTOR.
    --  If Task Reactor work items shall be removed after some time of not being taken up by the instance,
    --  then this default view can be replaced with the custom implementation
    --  or this parameter can be changed to the name of the custom view.
    --  You can pass the name of the view that will be created even after this procedure finishes its execution.
        NULL
    );

CREATE OR REPLACE VIEW EXAMPLE_CONNECTOR_TASK_REACTOR.WORK_SELECTOR_VIEW AS SELECT * FROM EXAMPLE_CONNECTOR_TASK_REACTOR.QUEUE ORDER BY RESOURCE_ID;

CREATE OR REPLACE PROCEDURE PUBLIC.RUN_SCHEDULER_ITERATION()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.integration.SchedulerIntegratedWithTaskReactorHandler.runIteration';

-----------------LIFECYCLE-----------------
CREATE OR REPLACE PROCEDURE PUBLIC.PAUSE_CONNECTOR()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.lifecycle.pause.PauseConnectorCustomHandler.pauseConnector';
GRANT USAGE ON PROCEDURE PUBLIC.PAUSE_CONNECTOR() TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.RESUME_CONNECTOR()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.lifecycle.resume.ResumeConnectorCustomHandler.resumeConnector';
GRANT USAGE ON PROCEDURE PUBLIC.RESUME_CONNECTOR() TO APPLICATION ROLE ADMIN;

-----------------RESOURCE MANAGEMENT-----------------
CREATE OR REPLACE PROCEDURE PUBLIC.CREATE_RESOURCE(
       name VARCHAR,
       resource_id VARIANT,
       ingestion_configurations VARIANT,
       id VARCHAR DEFAULT NULL,
       enabled BOOLEAN DEFAULT FALSE,
       resource_metadata VARIANT DEFAULT NULL)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.create.TemplateCreateResourceHandler.createResource';
GRANT USAGE ON PROCEDURE PUBLIC.CREATE_RESOURCE(VARCHAR, VARIANT, VARIANT, VARCHAR, BOOLEAN, VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.ENABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.enable.TemplateEnableResourceHandler.enableResource';
GRANT USAGE ON PROCEDURE PUBLIC.ENABLE_RESOURCE(VARCHAR) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.DISABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.disable.TemplateDisableResourceHandler.disableResource';
GRANT USAGE ON PROCEDURE PUBLIC.DISABLE_RESOURCE(VARCHAR) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_RESOURCE(
       resource_ingestion_definition_id VARCHAR,
       ingestion_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-template.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.update.TemplateUpdateResourceHandler.updateResource';
GRANT USAGE ON PROCEDURE PUBLIC.UPDATE_RESOURCE(VARCHAR, VARIANT) TO APPLICATION ROLE ADMIN;
