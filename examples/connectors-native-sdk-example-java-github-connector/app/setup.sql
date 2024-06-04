-- Copyright (c) 2024 Snowflake Inc.

-- CONNECTORS-NATIVE-SDK
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/all.sql';
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/task_reactor.sql';

-- CUSTOM CONNECTOR OBJECTS
CREATE OR ALTER VERSIONED SCHEMA STREAMLIT;
GRANT USAGE ON SCHEMA STREAMLIT TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE STREAMLIT STREAMLIT.EXAMPLE_JAVA_GITHUB_CONNECTOR_ST
    FROM  '/streamlit'
    MAIN_FILE = 'streamlit_app.py';
GRANT USAGE ON STREAMLIT STREAMLIT.EXAMPLE_JAVA_GITHUB_CONNECTOR_ST TO APPLICATION ROLE ADMIN;

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
                'GitHub account',
                'Prepare a GitHub account, from which particular resources will be fetched.',
                NULL,
                1
               ),
               ('2',
                'GitHub personal access token',
                'Prepare a personal GitHub access token in order to give the connector an access to the account.',
                NULL,
                2
               ),
               ('3',
                'External Access Integration',
                'It is required to create an External Access Integration that allows to connect with the Github API. To do that, you need to create a Secret with the Github personal access token and a Network Rule that allows to connect with __api.github.com:443__ endpoint.',
                'https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access',
                3
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

CREATE OR REPLACE VIEW EXAMPLE_CONNECTOR_TASK_REACTOR.WORK_SELECTOR_VIEW AS SELECT * FROM EXAMPLE_CONNECTOR_TASK_REACTOR.QUEUE ORDER BY RESOURCE_ID;

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
