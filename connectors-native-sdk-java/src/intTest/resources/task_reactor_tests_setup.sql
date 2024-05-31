-- Copyright (c) 2024 Snowflake Inc.

CREATE SCHEMA IF NOT EXISTS <test_schema>;

CREATE PROCEDURE <work_selector>(INPUT ARRAY)
    RETURNS ARRAY
    LANGUAGE SQL
    AS
    $$
    BEGIN
        RETURN INPUT;
    END;
    $$;

CREATE VIEW <expired_work_selector> AS
SELECT NULL AS ID WHERE 1 = 0;

CREATE TABLE <worker_procedure_calls>(
    WORKER_ID STRING,
    SCHEMA STRING,
    TIMESTAMP DATETIME DEFAULT SYSDATE()
    );

CREATE PROCEDURE <worker_procedure>(WORKER_ID NUMBER, SCHEMA STRING)
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    BEGIN
        INSERT INTO <worker_procedure_calls>(WORKER_ID, SCHEMA) VALUES (:WORKER_ID, :SCHEMA);
        RETURN 1;
    END;
    $$;

CREATE PROCEDURE <failing_worker_procedure>(WORKER_ID NUMBER, SCHEMA STRING)
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    DECLARE
        WORKER_PROCEDURE_EXCEPTION EXCEPTION;
    BEGIN
        RAISE WORKER_PROCEDURE_EXCEPTION;
    END;
    $$;
