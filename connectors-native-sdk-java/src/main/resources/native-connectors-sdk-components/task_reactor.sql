-- Copyright (c) 2024 Snowflake Inc.

CREATE OR ALTER VERSIONED SCHEMA TASK_REACTOR;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.DISPATCHER(INSTANCE_SCHEMA_NAME VARCHAR)
    RETURNS STRING
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.taskreactor.api.DispatcherHandler.dispatchWorkItems';

CREATE OR REPLACE PROCEDURE TASK_REACTOR.SET_WORKERS_NUMBER(WORKERS_NUMBER NUMBER, INSTANCE_SCHEMA_NAME VARCHAR)
    RETURNS STRING
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.taskreactor.api.SetWorkersNumberHandler.setWorkersNumber';

CREATE OR REPLACE PROCEDURE TASK_REACTOR.UPDATE_WAREHOUSE_INSTANCE(WAREHOUSE_NAME VARCHAR, INSTANCE_SCHEMA VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.taskreactor.api.UpdateWarehouseInstanceHandler.updateWarehouse';

CREATE OR REPLACE PROCEDURE TASK_REACTOR.PAUSE_INSTANCE(INSTANCE_SCHEMA VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.taskreactor.api.PauseInstanceHandler.pauseInstance';

CREATE OR REPLACE PROCEDURE TASK_REACTOR.RESUME_INSTANCE(INSTANCE_SCHEMA VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    PACKAGES = ('com.snowflake:snowpark:1.11.0')
    IMPORTS = ('/connectors-native-sdk.jar')
    HANDLER = 'com.snowflake.connectors.taskreactor.api.ResumeInstanceHandler.resumeInstance';

CREATE SCHEMA IF NOT EXISTS TASK_REACTOR_INSTANCES;

CREATE TABLE IF NOT EXISTS TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY
(
    INSTANCE_NAME  VARCHAR NOT NULL UNIQUE,
    IS_INITIALIZED BOOLEAN DEFAULT FALSE,
    IS_ACTIVE      BOOLEAN DEFAULT FALSE
);

CREATE OR REPLACE PROCEDURE TASK_REACTOR.create_instance_schema(
    instance_schema_name VARCHAR
)
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
            DECLARE
                schema_with_the_same_name_already_exists EXCEPTION (-20004, 'Schema with the same name already exists.');
            BEGIN
                CREATE SCHEMA IDENTIFIER(:instance_schema_name) MAX_DATA_EXTENSION_TIME_IN_DAYS = 90;
            EXCEPTION
                WHEN OTHER THEN
                    RAISE schema_with_the_same_name_already_exists;
            END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.validate_procedure_existence(
    procedure_name VARCHAR,
    procedure_type VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
            DECLARE
                row_counter INTEGER;
                schema_name VARCHAR;
                procedure_raw_name VARCHAR;
            BEGIN
                schema_name := SPLIT_PART(procedure_name, '.', 1);
                procedure_raw_name := SPLIT_PART(procedure_name, '.', 2);
                SELECT COUNT(*) INTO :row_counter FROM INFORMATION_SCHEMA.PROCEDURES
                    where 1=1
                        and PROCEDURE_SCHEMA ilike :schema_name
                        and PROCEDURE_NAME ilike :procedure_raw_name;
                IF(row_counter < 1) THEN
                    CASE (procedure_type)
                        WHEN 'WORKER_PROCEDURE' THEN
                            let sql varchar := 'declare worker_procedure_not_found_exception exception(-20001, ''Worker procedure ' || procedure_name || ' not found''); begin raise worker_procedure_not_found_exception; end;';
                             execute immediate :sql;
                        WHEN 'WORK_SELECTOR_PROCEDURE' THEN
                            let sql varchar := 'declare work_selector_procedure_not_found_exception exception(-20002, ''Work selector procedure ' || procedure_name || ' not found''); begin raise work_selector_procedure_not_found_exception; end;';
                            execute immediate :sql;
                        ELSE
                            let sql varchar := 'declare default_procedure_validation_exception exception(-20003, ''Procedure' || procedure_name || 'not found''); begin raise default_procedure_validation_exception; end;';
                            execute immediate :sql;
                    END CASE;
                END IF;
            END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.create_queue(
    instance_schema_name VARCHAR,
    table_name VARCHAR,
    stream_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_table_name VARCHAR;
            complex_stream_name VARCHAR;
        BEGIN
            complex_table_name := instance_schema_name || '.' || table_name;
            CREATE TABLE IF NOT EXISTS IDENTIFIER(:complex_table_name) (
                ID STRING DEFAULT UUID_STRING(),
                RESOURCE_ID STRING NOT NULL,
                DISPATCHER_OPTIONS VARIANT DEFAULT NULL,
                WORKER_PAYLOAD VARIANT DEFAULT OBJECT_CONSTRUCT(),
                TIMESTAMP DATETIME DEFAULT SYSDATE()
            );
            complex_stream_name := instance_schema_name || '.' || stream_name;
            CREATE STREAM IF NOT EXISTS IDENTIFIER(:complex_stream_name) ON TABLE IDENTIFIER(:complex_table_name);
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.create_worker_registry_sequence(
    instance_schema_name VARCHAR,
    sequence_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_sequence_name VARCHAR;
        BEGIN
            complex_sequence_name := instance_schema_name || '.' || sequence_name;
            CREATE SEQUENCE IF NOT EXISTS identifier(:complex_sequence_name) ORDER;
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.create_worker_registry(
    instance_schema_name VARCHAR,
    table_name VARCHAR,
    sequence_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_table_name VARCHAR;
            complex_sequence_name VARCHAR;
            query VARCHAR;
        BEGIN
            complex_table_name := instance_schema_name || '.' || table_name;
            complex_sequence_name := instance_schema_name || '.' || sequence_name;
            query := 'CREATE TABLE IF NOT EXISTS ' || :complex_table_name || ' (WORKER_ID NUMBER DEFAULT ' || :complex_sequence_name || '.nextval, CREATED_AT DATETIME DEFAULT SYSDATE(), UPDATED_AT DATETIME DEFAULT SYSDATE(), STATUS STRING);';
            execute immediate :query;
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.create_worker_status_table(
    instance_schema_name VARCHAR,
    table_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_table_name VARCHAR;
        BEGIN
            complex_table_name := instance_schema_name || '.' || table_name;
            CREATE TABLE IF NOT EXISTS identifier(:complex_table_name) (WORKER_ID NUMBER, TIMESTAMP DATETIME DEFAULT SYSDATE(), STATUS STRING);
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.create_config_table(
    instance_schema_name VARCHAR,
    table_name VARCHAR,
    worker_procedure_name VARCHAR,
    work_selector_type VARCHAR,
    work_selector_name VARCHAR,
    expired_work_selector_name VARCHAR,
    is_instance_registered BOOLEAN
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_table_name VARCHAR;
        BEGIN
            complex_table_name := instance_schema_name || '.' || table_name;
            CREATE TABLE IF NOT EXISTS identifier(:complex_table_name) (KEY VARCHAR, VALUE VARCHAR);

            -- insert config data only when instance is not registered yet
            IF(NOT :is_instance_registered) THEN
                INSERT INTO identifier(:complex_table_name) (KEY, VALUE) VALUES
                    ('WORKER_PROCEDURE', :worker_procedure_name),
                    ('WORK_SELECTOR_TYPE', :work_selector_type),
                    ('WORK_SELECTOR', :work_selector_name),
                    ('EXPIRED_WORK_SELECTOR', :expired_work_selector_name),
                    ('SCHEMA', :instance_schema_name),
                    ('LAST_STREAMS_RECREATION', TO_VARCHAR(SYSDATE()));
            END IF;
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.CREATE_COMMANDS_QUEUE(
    instance_schema_name VARCHAR,
    commands_queue_name VARCHAR,
    commands_queue_sequence_name VARCHAR,
    commands_queue_stream_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_sequence_name VARCHAR := instance_schema_name || '.' || commands_queue_sequence_name;
            complex_table_name VARCHAR := instance_schema_name || '.' || commands_queue_name;
            complex_stream_name VARCHAR := instance_schema_name || '.' || commands_queue_stream_name;
        BEGIN
            CREATE SEQUENCE IF NOT EXISTS identifier(:complex_sequence_name) ORDER;

            let query VARCHAR := 'CREATE TABLE IF NOT EXISTS ' || :complex_table_name || ' (ID VARCHAR NOT NULL UNIQUE DEFAULT UUID_STRING(), TYPE VARCHAR NOT NULL, PAYLOAD VARIANT, SEQ_NO NUMBER DEFAULT ' || :complex_sequence_name || '.NEXTVAL);';
            execute immediate :query;

            CREATE STREAM IF NOT EXISTS IDENTIFIER(:complex_stream_name) ON TABLE IDENTIFIER(:complex_table_name);
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.CREATE_INSTANCE_OBJECTS(
    instance_schema_name VARCHAR,
    worker_procedure_name VARCHAR,
    work_selector_type VARCHAR,
    work_selector_name VARCHAR,
    expired_work_selector_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            is_instance_registered BOOLEAN DEFAULT 0<(SELECT COUNT(1) FROM TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY WHERE INSTANCE_NAME = UPPER(:instance_schema_name));
        BEGIN
            -- instance schema is created and procedure existence checks are done only when instance is not registered yet
            IF(NOT :is_instance_registered) THEN
                call TASK_REACTOR.create_instance_schema(
                        :instance_schema_name
                    );
                call TASK_REACTOR.validate_procedure_existence(
                        :worker_procedure_name,
                        'WORKER_PROCEDURE'
                    );

                IF (:work_selector_type = 'PROCEDURE') THEN
                    call TASK_REACTOR.validate_procedure_existence(
                        :work_selector_name,
                        'WORK_SELECTOR_PROCEDURE'
                    );
                END IF;
            END IF;

            -- create default expired work selector
            IF (:expired_work_selector_name IS NULL) THEN
                expired_work_selector_name := 'TASK_REACTOR.EMPTY_EXPIRED_WORK_SELECTOR';
                CREATE OR REPLACE VIEW TASK_REACTOR.EMPTY_EXPIRED_WORK_SELECTOR AS
                    SELECT NULL AS ID WHERE 1 = 0;
            END IF;

            -- create or update instance objects
            call TASK_REACTOR.create_queue(
                :instance_schema_name,
                'QUEUE',
                'QUEUE_STREAM'
            );
            call TASK_REACTOR.create_commands_queue(
                :instance_schema_name,
                'COMMANDS_QUEUE',
                'COMMANDS_QUEUE_SEQUENCE',
                'COMMANDS_QUEUE_STREAM'
                );
            call TASK_REACTOR.create_worker_registry_sequence(
                :instance_schema_name,
                'WORKER_REGISTRY_SEQUENCE'
                );
            call TASK_REACTOR.create_worker_registry(
                :instance_schema_name,
                'WORKER_REGISTRY',
                'WORKER_REGISTRY_SEQUENCE'
            );
            call TASK_REACTOR.create_worker_status_table(
                :instance_schema_name,
                'WORKER_STATUS'
            );
            call TASK_REACTOR.create_config_table(
                    :instance_schema_name,
                    'CONFIG',
                    :worker_procedure_name,
                    :work_selector_type,
                    :work_selector_name,
                    :expired_work_selector_name,
                    :is_instance_registered
            );

            -- register new instance only if all instance objects are created
            IF(NOT :is_instance_registered) THEN
                INSERT INTO TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY (INSTANCE_NAME)
                    VALUES (UPPER(:instance_schema_name));
            END IF;
        END;
        $$;

CREATE OR REPLACE PROCEDURE TASK_REACTOR.INITIALIZE_INSTANCE(
    instance_schema_name VARCHAR,
    warehouse_name VARCHAR,
    dt_should_be_started BOOLEAN,
    dt_task_schedule VARCHAR,
    dt_allow_overlapping_execution BOOLEAN,
    dt_user_task_timeout_ms VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
        AS
        $$
        DECLARE
            complex_task_name                  VARCHAR;
            complex_config_table_name          VARCHAR;
            complex_queue_stream_name          VARCHAR;
            complex_commands_queue_stream_name VARCHAR;
            create_task_sql                    VARCHAR;
            udf_warehouse                      VARCHAR;
            task_schedule                      VARCHAR;
            allow_overlapping_execution        VARCHAR;
            should_be_started                  BOOLEAN;
            user_task_timeout_ms               VARCHAR;
            task_condition                     VARCHAR;
            task_definition                    VARCHAR;
            is_initialized                     BOOLEAN;
            capitalized_instance_name          VARCHAR DEFAULT UPPER(:instance_schema_name);
            res RESULTSET DEFAULT (SELECT IS_INITIALIZED
                                   FROM TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY
                                   WHERE INSTANCE_NAME = :capitalized_instance_name);
            cur CURSOR for res;
        BEGIN
            complex_config_table_name := instance_schema_name || '.CONFIG';
            complex_queue_stream_name := instance_schema_name || '.QUEUE_STREAM';
            complex_commands_queue_stream_name := instance_schema_name || '.COMMANDS_QUEUE_STREAM';

            --prepare the cursor and fetch the required data
            OPEN cur;
            FETCH cur INTO is_initialized;

            --check if an instance with the given name exists
            IF (:is_initialized is null) THEN
                LET instance_not_found_msg VARCHAR := 'Instance with name: \\\'' || :instance_schema_name || '\\\' does not exist.';
                LET statement VARCHAR := 'DECLARE INSTANCE_NOT_FOUND EXCEPTION(-20001, ''' || :instance_not_found_msg || '''); BEGIN RAISE INSTANCE_NOT_FOUND; END;';
                EXECUTE IMMEDIATE :statement;
            END IF;

            --check if the instance is already initialized
            IF(:is_initialized) THEN
                LET instance_already_initialized_msg VARCHAR := 'Instance with name: ' || :instance_schema_name || ' is already initialized.';
                LET statement VARCHAR := 'DECLARE INSTANCE_ALREADY_INITIALIZED EXCEPTION(-20002, ''' || :instance_already_initialized_msg || '''); BEGIN RAISE INSTANCE_ALREADY_INITIALIZED; END;';
                EXECUTE IMMEDIATE :statement;
            END IF;

            --assemble statement instructions
            complex_task_name := instance_schema_name || '.DISPATCHER_TASK ';
            udf_warehouse := IFF(warehouse_name is null, '', 'WAREHOUSE = ' || warehouse_name || ' ');
            task_schedule := IFF(dt_task_schedule is null, 'SCHEDULE = \'1 MINUTE\' ', 'SCHEDULE = \'' || dt_task_schedule || '\' ');
            allow_overlapping_execution := IFF(dt_allow_overlapping_execution is null, 'ALLOW_OVERLAPPING_EXECUTION = FALSE ', 'ALLOW_OVERLAPPING_EXECUTION = ' || dt_allow_overlapping_execution || ' ');
            should_be_started := IFF(dt_should_be_started is null, TRUE, dt_should_be_started);
            user_task_timeout_ms := IFF(dt_user_task_timeout_ms is null, '', 'user_task_timeout_ms = ' || dt_user_task_timeout_ms || ' ');
            task_condition := 'WHEN SYSTEM$STREAM_HAS_DATA(\'' || :complex_queue_stream_name || '\') OR SYSTEM$STREAM_HAS_DATA(\'' || :complex_commands_queue_stream_name || '\') ';
            task_definition := 'AS CALL TASK_REACTOR.DISPATCHER(\'' || :instance_schema_name ||'\')';

            --create dispatcher task
            create_task_sql := 'CREATE TASK ' || :complex_task_name || :udf_warehouse || :task_schedule || :allow_overlapping_execution || :user_task_timeout_ms || :task_condition || :task_definition || ';';
            execute immediate :create_task_sql;

            --start the task if required
            IF(:should_be_started) THEN
                ALTER TASK identifier (:complex_task_name) RESUME;
                UPDATE TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY
                SET IS_INITIALIZED = TRUE,
                    IS_ACTIVE      = TRUE
                WHERE INSTANCE_NAME = :capitalized_instance_name;
            ELSE
                UPDATE TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY
                SET IS_INITIALIZED = TRUE
                WHERE INSTANCE_NAME = :capitalized_instance_name;
            END IF;

            --update instance config with warehouse property
            INSERT INTO identifier(:complex_config_table_name) (KEY, VALUE) VALUES
                ('WAREHOUSE', :warehouse_name);

            RETURN OBJECT_CONSTRUCT('response_code', 'OK', 'message', 'Instance has been initialized successfully.');
        END;
        $$;
