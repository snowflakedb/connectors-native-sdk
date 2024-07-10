-- Copyright (c) 2024 Snowflake Inc.

-- CONNECTORS-NATIVE-SDK
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/all.sql';
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/task_reactor.sql';

-- CUSTOM CONNECTOR OBJECTS
CREATE OR ALTER VERSIONED SCHEMA STREAMLIT;
GRANT USAGE ON SCHEMA STREAMLIT TO APPLICATION ROLE ADMIN;
GRANT USAGE ON SCHEMA STREAMLIT TO APPLICATION ROLE DATA_READER;
GRANT USAGE ON SCHEMA STREAMLIT TO APPLICATION ROLE VIEWER;

CREATE OR REPLACE STREAMLIT STREAMLIT.EXAMPLE_JAVA_GITHUB_CONNECTOR_ST
    FROM  '/streamlit'
    MAIN_FILE = 'streamlit_app.py';
GRANT USAGE ON STREAMLIT STREAMLIT.EXAMPLE_JAVA_GITHUB_CONNECTOR_ST TO APPLICATION ROLE ADMIN;

-- SNOWFLAKE REFERENCE MECHANISM
CREATE OR REPLACE PROCEDURE PUBLIC.GET_REFERENCE_CONFIG(ref_name STRING)
    RETURNS STRING
    LANGUAGE SQL
    AS
        BEGIN
            CASE (ref_name)
                WHEN 'GITHUB_EAI_REFERENCE' THEN
                    RETURN OBJECT_CONSTRUCT(
                        'type', 'CONFIGURATION',
                        'payload', OBJECT_CONSTRUCT(
                            'host_ports', ARRAY_CONSTRUCT('api.github.com'),
                            'allowed_secrets', 'LIST',
                            'secret_references', ARRAY_CONSTRUCT('GITHUB_SECRET_REFERENCE')
                        )
                    )::STRING;
                WHEN 'GITHUB_SECRET_REFERENCE' THEN
                    RETURN OBJECT_CONSTRUCT(
                        'type', 'CONFIGURATION',
                        'payload', OBJECT_CONSTRUCT(
                            'type', 'OAUTH2',
                            'security_integration', OBJECT_CONSTRUCT(
                                'oauth_scopes', ARRAY_CONSTRUCT('repo'),
                                'oauth_token_endpoint', 'https://github.com/login/oauth/access_token',
                                'oauth_authorization_endpoint', 'https://github.com/login/oauth/authorize'
                            )
                        )
                    )::STRING;
                ELSE
                    RETURN '';
            END CASE;
        END;
GRANT USAGE ON PROCEDURE PUBLIC.GET_REFERENCE_CONFIG(STRING) TO APPLICATION ROLE ADMIN;

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
                'GitHub account',
                'Prepare a GitHub account which will be used to fetch the data from repositories',
                NULL,
                1
               ),
               ('2',
                'GitHub repositories access',
                'Make sure your GitHub account can access all repositories which you would like to ingest',
                NULL,
                2
               )
    ) AS src (id, title, description, documentation_url, position)
    ON dest.id = src.id
    WHEN NOT MATCHED THEN
        INSERT (id, title, description, documentation_url, position)
        VALUES (src.id, src.title, src.description, src.documentation_url, src.position);

-- CONNECTION CONFIGURATION
CREATE OR REPLACE PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION(connection_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.configuration.connection.GithubConnectionConfigurationHandler.setConnectionConfiguration';
GRANT USAGE ON PROCEDURE PUBLIC.SET_CONNECTION_CONFIGURATION(VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.TEST_CONNECTION()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.configuration.connection.GithubConnectionValidator.testConnection';

-- FINALIZE CONFIGURATION
CREATE OR REPLACE PROCEDURE PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION(CUSTOM_CONFIGURATION VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.configuration.finalize.FinalizeConnectorConfigurationCustomHandler.finalizeConnectorConfiguration';
GRANT USAGE ON PROCEDURE PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION(VARIANT) TO APPLICATION ROLE ADMIN;

-----------------TASK REACTOR-----------------
CREATE OR REPLACE PROCEDURE PUBLIC.GITHUB_WORKER(worker_id number, task_reactor_schema string)
    RETURNS STRING
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0', 'com.snowflake:telemetry:latest')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-example-java-github-connector.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.GitHubWorker.executeWork';

CALL TASK_REACTOR.CREATE_INSTANCE_OBJECTS(
        'EXAMPLE_CONNECTOR_TASK_REACTOR',
        'PUBLIC.GITHUB_WORKER',
        'VIEW',
        'EXAMPLE_CONNECTOR_TASK_REACTOR.WORK_SELECTOR_VIEW',
        NULL
    );

CREATE OR REPLACE VIEW EXAMPLE_CONNECTOR_TASK_REACTOR.WORK_SELECTOR_VIEW
    AS SELECT * FROM EXAMPLE_CONNECTOR_TASK_REACTOR.QUEUE ORDER BY RESOURCE_ID;

CREATE OR REPLACE PROCEDURE PUBLIC.RUN_SCHEDULER_ITERATION()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-example-java-github-connector.jar')
    HANDLER = 'com.snowflake.connectors.example.integration.SchedulerIntegratedWithTaskReactorHandler.runIteration';

-----------------LIFECYCLE-----------------
CREATE OR REPLACE PROCEDURE PUBLIC.PAUSE_CONNECTOR()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-example-java-github-connector.jar')
    HANDLER = 'com.snowflake.connectors.example.lifecycle.pause.PauseConnectorCustomHandler.pauseConnector';
GRANT USAGE ON PROCEDURE PUBLIC.PAUSE_CONNECTOR() TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.RESUME_CONNECTOR()
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar', '/connectors-native-sdk-example-java-github-connector.jar')
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
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.create.CreateRepoResourceHandler.createResource';
GRANT USAGE ON PROCEDURE PUBLIC.CREATE_RESOURCE(VARCHAR, VARIANT, VARIANT, VARCHAR, BOOLEAN, VARIANT) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.ENABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.enable.EnableRepoResourceHandler.enableResource';
GRANT USAGE ON PROCEDURE PUBLIC.ENABLE_RESOURCE(VARCHAR) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.DISABLE_RESOURCE(resource_ingestion_definition_id VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.disable.DisableRepoResourceHandler.disableResource';
GRANT USAGE ON PROCEDURE PUBLIC.DISABLE_RESOURCE(VARCHAR) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_RESOURCE(
       resource_ingestion_definition_id VARCHAR,
       ingestion_configuration VARIANT)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk-example-java-github-connector.jar', '/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.example.ingestion.update.UpdateRepoResourceHandler.updateResource';
GRANT USAGE ON PROCEDURE PUBLIC.UPDATE_RESOURCE(VARCHAR, VARIANT) TO APPLICATION ROLE ADMIN;
