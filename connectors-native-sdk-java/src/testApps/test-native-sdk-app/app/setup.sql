-- Copyright (c) 2024 Snowflake Inc.

EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/all.sql';
EXECUTE IMMEDIATE FROM 'native-connectors-sdk-components/task_reactor.sql';

-- Execute any query as an application and return the results
-- Used to change internal app data during testing
CREATE OR REPLACE PROCEDURE PUBLIC.EXECUTE_SQL(query STRING)
    RETURNS TABLE()
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        results RESULTSET;
    BEGIN
        results := (EXECUTE IMMEDIATE :query);
        RETURN TABLE(results);
    END;
GRANT USAGE ON PROCEDURE PUBLIC.EXECUTE_SQL(STRING) TO APPLICATION ROLE ADMIN;

-- Update any procedure to be a simple mock, returning the specified response
CREATE OR REPLACE PROCEDURE PUBLIC.MOCK_PROCEDURE(procedure STRING, response VARIANT)
    RETURNS BOOLEAN
    LANGUAGE SQL
    AS
    DECLARE
        query STRING;
    BEGIN
        query := 'CREATE OR REPLACE PROCEDURE ' || :procedure || '
                      RETURNS VARIANT
                      LANGUAGE SQL
                      AS
                      BEGIN
                          RETURN PARSE_JSON(\'' || :response::STRING || '\');
                      END';
        EXECUTE IMMEDIATE :query;
    END;
GRANT USAGE ON PROCEDURE PUBLIC.MOCK_PROCEDURE(STRING, VARIANT) TO APPLICATION ROLE ADMIN;

-- Update any procedure to be a simple mock, containing the specified body
CREATE OR REPLACE PROCEDURE PUBLIC.MOCK_PROCEDURE_WITH_BODY(procedure STRING, body STRING)
    RETURNS BOOLEAN
    LANGUAGE SQL
    AS
    DECLARE
        query STRING;
    BEGIN
        query := 'CREATE OR REPLACE PROCEDURE ' || :procedure || '
                      RETURNS VARIANT
                      LANGUAGE SQL
                      AS
                      BEGIN
                          ' || :body || ';
                      END';
        EXECUTE IMMEDIATE :query;
    END;
GRANT USAGE ON PROCEDURE PUBLIC.MOCK_PROCEDURE_WITH_BODY(STRING, STRING) TO APPLICATION ROLE ADMIN;

-- Update any procedure to be a simple mock, calling the specified Java handler
CREATE OR REPLACE PROCEDURE PUBLIC.MOCK_PROCEDURE_WITH_HANDLER(procedure STRING, handler STRING)
    RETURNS BOOLEAN
    LANGUAGE SQL
    AS
    DECLARE
        query STRING;
    BEGIN
        query := 'CREATE OR REPLACE PROCEDURE ' || :procedure || '
                      RETURNS VARIANT
                      LANGUAGE JAVA
                      RUNTIME_VERSION = ''11''
                      PACKAGES = (''com.snowflake:snowpark:1.11.0'')
                      IMPORTS = (''/connectors-native-sdk.jar'', ''/test-native-sdk-app.jar'')
                      HANDLER = ''' || :handler || '''';
        EXECUTE IMMEDIATE :query;
    END;
GRANT USAGE ON PROCEDURE PUBLIC.MOCK_PROCEDURE_WITH_HANDLER(STRING, STRING) TO APPLICATION ROLE ADMIN;

-- Update any procedure to be a simple mock, throwing an exception with the specified message
CREATE OR REPLACE PROCEDURE PUBLIC.MOCK_PROCEDURE_TO_THROW(procedure STRING, message STRING)
    RETURNS BOOLEAN
    LANGUAGE SQL
    AS
    DECLARE
        query STRING;
    BEGIN
        query := 'CREATE OR REPLACE PROCEDURE ' || :procedure || '
                      RETURNS VARIANT
                      LANGUAGE SQL
                      AS
                      DECLARE
                          MOCK_EXCEPTION EXCEPTION(-20002, \'Mock exception message\');
                      BEGIN
                          RAISE MOCK_EXCEPTION;
                      END';
        EXECUTE IMMEDIATE :query;
    END;
GRANT USAGE ON PROCEDURE PUBLIC.MOCK_PROCEDURE_TO_THROW(STRING, STRING) TO APPLICATION ROLE ADMIN;

-- Drop any procedure
CREATE OR REPLACE PROCEDURE PUBLIC.DROP_PROCEDURE(procedure STRING)
    RETURNS BOOLEAN
    LANGUAGE SQL
    AS
    DECLARE
        query STRING;
    BEGIN
        query := 'DROP PROCEDURE ' || procedure;
        EXECUTE IMMEDIATE :query;
    END;
GRANT USAGE ON PROCEDURE PUBLIC.DROP_PROCEDURE(STRING) TO APPLICATION ROLE ADMIN;

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

CREATE OR REPLACE PROCEDURE PUBLIC.TEST_CONNECTION()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        RETURN OBJECT_CONSTRUCT('response_code', 'OK', 'message', 'This is TEST_CONNECTION mock only procedure returning OK response code');
    END;

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

-- TASK REACTOR
CREATE OR REPLACE PROCEDURE PUBLIC.TEST_WORKER(worker_id number, task_reactor_schema string)
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    BEGIN
        return "OK";
    END;
    $$;

CALL TASK_REACTOR.CREATE_INSTANCE_OBJECTS(
        'TR_INSTANCE',
        'PUBLIC.TEST_WORKER',
        'VIEW',
        'TR_INSTANCE.WORK_SELECTOR_VIEW',
        NULL
    );

CREATE OR REPLACE VIEW TR_INSTANCE.WORK_SELECTOR_VIEW AS SELECT * FROM TR_INSTANCE.QUEUE;
