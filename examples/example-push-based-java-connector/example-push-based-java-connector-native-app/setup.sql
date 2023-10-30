CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
CREATE OR ALTER VERSIONED SCHEMA PUBLIC;
GRANT USAGE ON SCHEMA PUBLIC TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE SCHEMA TASKS;
GRANT USAGE ON SCHEMA TASKS TO APPLICATION ROLE APP_PUBLIC;


----------
-- STATE
----------
CREATE SCHEMA IF NOT EXISTS STATE;
GRANT USAGE ON SCHEMA STATE TO APPLICATION ROLE APP_PUBLIC;

CREATE TABLE IF NOT EXISTS STATE.APP_CONFIGURATION (
    KEY STRING NOT NULL,
    VALUE VARIANT NOT NULL
);
GRANT SELECT ON TABLE STATE.APP_CONFIGURATION TO APPLICATION ROLE APP_PUBLIC;

CREATE TABLE STATE.RESOURCE_CONFIGURATION(
         KEY STRING,
         VALUE VARIANT
);
CREATE TABLE STATE.APP_STATE(
    TIMESTAMP TIMESTAMP_NTZ DEFAULT SYSDATE(),
    KEY STRING,
    VALUE VARIANT
);

---------

CREATE OR REPLACE PROCEDURE PUBLIC.INIT_DESTINATION_DATABASE(database_name string)
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        -- Creating destination database
        CREATE DATABASE IF NOT EXISTS IDENTIFIER(:database_name);
        GRANT USAGE ON DATABASE IDENTIFIER(:database_name) TO APPLICATION ROLE APP_PUBLIC;
        -- Creating schema
        LET destination_schema_name string := :database_name || '.PUBLIC';
        CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:destination_schema_name);
        GRANT USAGE ON SCHEMA IDENTIFIER(:destination_schema_name) TO APPLICATION ROLE APP_PUBLIC;
        INSERT INTO STATE.APP_CONFIGURATION SELECT 'config', OBJECT_CONSTRUCT('destination_database', :database_name);
        RETURN 'Database initialised!';
    END;
GRANT USAGE ON PROCEDURE PUBLIC.INIT_DESTINATION_DATABASE(string) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE PUBLIC.INIT_RESOURCE(table_name string, database_name string)
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        LET delta_table_name string := :database_name || '.PUBLIC.' || :table_name || '_DELTA';
        LET base_table_name string := :database_name || '.PUBLIC.' || :table_name || '_BASE';
        LET delta_stream_name string := :database_name || '.PUBLIC.STRM_' || :table_name || '_DELTA';
        LET merged_view_name string := :database_name || '.PUBLIC.VIEW_' || :table_name || '_MERGED';
        LET merge_task_name string := 'TASKS.MERGE_' || :table_name || '_TASK';

        -- Creating delta table
        CREATE TABLE IF NOT EXISTS IDENTIFIER(:delta_table_name) (
            ID INTEGER,
            COL_1 VARCHAR,
            COL_2 VARCHAR,
            TS TIMESTAMP_LTZ
        );
        -- Creating base table
        CREATE TABLE IF NOT EXISTS IDENTIFIER(:base_table_name) (
            ID INTEGER,
            COL_1 VARCHAR,
            COL_2 VARCHAR,
            OPERATION VARCHAR
        );

        -- Creating stream on delta table
        CREATE STREAM IF NOT EXISTS IDENTIFIER(:delta_stream_name) ON TABLE IDENTIFIER(:delta_table_name) APPEND_ONLY=TRUE;

        -- Creating view with merged data from base and delta tables
        CREATE VIEW IF NOT EXISTS IDENTIFIER(:merged_view_name)
        AS
            WITH CTE_NET_DELTA AS (
                SELECT ID,
                       COL_1,
                       COL_2
                  FROM IDENTIFIER(:delta_stream_name)
                QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY TS DESC)
            )
            SELECT base.ID,
                   base.COL_1,
                   base.COL_2
                FROM IDENTIFIER(:base_table_name) base
            WHERE NOT EXISTS (
                SELECT 1
                FROM CTE_NET_DELTA D
                WHERE D.ID = base.ID
            )
            UNION ALL
            SELECT D.ID,
                   D.COL_1,
                   D.COL_2
                FROM CTE_NET_DELTA D
        ;

        -- Creating task which merges data from delta table to base table
        let create_task_command string :=
        '
        CREATE TASK IF NOT EXISTS ' || :merge_task_name || '
            SCHEDULE = ''1 MINUTE''
            SUSPEND_TASK_AFTER_NUM_FAILURES = 2
            WHEN SYSTEM$STREAM_HAS_DATA( ''' || :delta_stream_name || ''')
        AS
        BEGIN
            let current_time := SYSDATE();
            MERGE INTO ' || :base_table_name || ' base
            USING (
                SELECT ID,
                       COL_1,
                       COL_2
                FROM ' || :delta_stream_name || '
                WHERE TS <= :current_time
                QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY TS DESC)
            ) delta
            ON delta.ID = base.ID
            WHEN NOT MATCHED THEN INSERT (ID, COL_1, COL_2, OPERATION) VALUES (delta.ID, delta.COL_1, delta.COL_2, ''INSERT'')
            WHEN MATCHED THEN UPDATE SET COL_1 = delta.COL_1, COL_2 = delta.COL_2, OPERATION = ''UPDATE'';
            DELETE FROM ' || :delta_table_name || ' WHERE TS <= :current_time;
        END;
        ';

        EXECUTE IMMEDIATE :create_task_command;

        -- Granting privileges on created objects
        GRANT SELECT, INSERT ON TABLE IDENTIFIER(:delta_table_name) TO APPLICATION ROLE APP_PUBLIC;
        GRANT SELECT, INSERT, UPDATE ON TABLE IDENTIFIER(:base_table_name) TO APPLICATION ROLE APP_PUBLIC;
        GRANT SELECT ON VIEW IDENTIFIER(:merged_view_name) TO APPLICATION ROLE APP_PUBLIC;
        GRANT SELECT ON STREAM IDENTIFIER(:delta_stream_name) TO APPLICATION ROLE APP_PUBLIC;

        GRANT ALL PRIVILEGES ON TASK IDENTIFIER(:merge_task_name) TO APPLICATION ROLE APP_PUBLIC;

        -- Starting task
        ALTER TASK IDENTIFIER(:merge_task_name) RESUME;
        EXECUTE TASK IDENTIFIER(:merge_task_name);

        RETURN 'Resource initialised!';
    END;
GRANT USAGE ON PROCEDURE PUBLIC.INIT_RESOURCE(string, string) TO APPLICATION ROLE APP_PUBLIC;

-- UI
CREATE OR REPLACE STREAMLIT PUBLIC.EXAMPLE_PUSH_BASED_JAVA_CONNECTOR_STREAMLIT
    FROM  '/'
    MAIN_FILE = 'streamlit_app.py';

GRANT USAGE ON STREAMLIT PUBLIC.EXAMPLE_PUSH_BASED_JAVA_CONNECTOR_STREAMLIT TO APPLICATION ROLE APP_PUBLIC;
