-- Copyright (c) 2024 Snowflake Inc.
CREATE OR REPLACE PROCEDURE PUBLIC.SETUP_EXTERNAL_INTEGRATION_WITH_NAMES(methods ARRAY)
    RETURNS VARIANT
    LANGUAGE SQL
    COMMENT = 'Applies SECRET and EXTERNAL ACCESS INTEGRATION references to list of Snowflake Procedures/Functions.'
    AS
    DECLARE
        response VARIANT;
        connection_config VARIANT;
    BEGIN
        --fetch connection config
        SELECT VALUE INTO :connection_config FROM STATE.APP_CONFIG WHERE KEY = 'connection_configuration';
        CALL PUBLIC.SETUP_EXTERNAL_INTEGRATION(
            AS_VARCHAR(:connection_config:external_access_integration),
            AS_VARCHAR(:connection_config:secret),
            :methods
        ) INTO :response;
        RETURN :response;
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.SETUP_EXTERNAL_INTEGRATION_WITH_REFERENCES(methods ARRAY)
    RETURNS VARIANT
    LANGUAGE SQL
    COMMENT = 'Applies SECRET and EXTERNAL ACCESS INTEGRATION references to list of Snowflake Procedures/Functions.'
    AS
    DECLARE
        response VARIANT;
    BEGIN
    CALL PUBLIC.SETUP_EXTERNAL_INTEGRATION(
                'reference(\'EAI_REFERENCE\')',
                'reference(\'SECRET_REFERENCE\')',
                :methods
            ) INTO :response;
    RETURN :response;
    END;

CREATE OR REPLACE PROCEDURE PUBLIC.SETUP_EXTERNAL_INTEGRATION(eai_idf VARCHAR, secret_idf VARCHAR, methods ARRAY)
    RETURNS VARIANT
    LANGUAGE SQL
    COMMENT = 'Applies SECRET and EXTERNAL ACCESS INTEGRATION passed as arguments to list of Snowflake Procedures/Functions.'
    AS
    DECLARE
        counter INTEGER DEFAULT 0;
        alter_proc_response VARIANT;
        alter_func_response VARIANT;
    BEGIN
        FOR counter IN 0 TO ARRAY_SIZE(:methods)-1 DO
            -- alter if method type is procedure
            CALL PUBLIC.SETUP_METHOD_EXTERNAL_INTEGRATION(:eai_idf, :secret_idf, :methods[:counter], 'PROCEDURE') INTO :alter_proc_response;
            IF (:alter_proc_response:response_code in ('EAI_UNAVAILABLE', 'SECRET_UNAVAILABLE')) THEN
                RETURN :alter_proc_response;
            END IF;

            -- alter if method type is function
            CALL PUBLIC.SETUP_METHOD_EXTERNAL_INTEGRATION(:eai_idf, :secret_idf, :methods[:counter], 'FUNCTION') INTO :alter_func_response;
            IF (:alter_func_response:response_code in ('EAI_UNAVAILABLE', 'SECRET_UNAVAILABLE')) THEN
                RETURN :alter_proc_response;
            END IF;
        END FOR;
        RETURN OBJECT_CONSTRUCT('response_code', 'OK', 'message', CONCAT('Successfully set up ', ARRAY_SIZE(:methods), ' method(s).'));
    END;


CREATE OR REPLACE PROCEDURE PUBLIC.SETUP_METHOD_EXTERNAL_INTEGRATION(eai_idf VARCHAR, secret_idf VARCHAR, method_idf VARCHAR, method_type VARCHAR)
    RETURNS VARIANT
    LANGUAGE SQL
    COMMENT = 'Applies SECRET and EXTERNAL ACCESS INTEGRATION references to Snowflake function/procedure.'
    AS
    BEGIN
        LET alter_method_statement VARCHAR := 'ALTER ' || :method_type || ' ' || :method_idf || ' SET SECRETS=(\'credentials\' = ' || :secret_idf || ') ' || 'EXTERNAL_ACCESS_INTEGRATIONS=(' || :eai_idf || ');';
        EXECUTE IMMEDIATE :alter_method_statement;
        RETURN OBJECT_CONSTRUCT('response_code', 'OK');
    EXCEPTION
        WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
            LET MESSAGE VARCHAR := 'Unable to apply SECRET and EXTERNAL_OBJECT_INTEGRATION objects to provided methods.';
            CASE
                WHEN CONTAINS(SQLERRM, 'SQL compilation error:\nIntegration') THEN
                    RETURN OBJECT_CONSTRUCT('response_code', 'EAI_UNAVAILABLE', 'message', :MESSAGE, 'SQLCODE', SQLCODE, 'SQLERRM', SQLERRM, 'SQLSTATE', SQLSTATE);
                WHEN CONTAINS(SQLERRM, 'SQL compilation error:\nSecret') THEN
                    RETURN OBJECT_CONSTRUCT('response_code', 'SECRET_UNAVAILABLE', 'message', :MESSAGE, 'SQLCODE', SQLCODE, 'SQLERRM', SQLERRM, 'SQLSTATE', SQLSTATE);
                ELSE
                    RETURN OBJECT_CONSTRUCT('response_code', 'INTERNAL_ERROR', 'message', :MESSAGE, 'SQLCODE', SQLCODE, 'SQLERRM', SQLERRM, 'SQLSTATE', SQLSTATE);
            END CASE;
        WHEN OTHER THEN
            LET MESSAGE VARCHAR := 'Procedure execution failed with an unknown error.';
            RETURN OBJECT_CONSTRUCT('response_code', 'INTERNAL_ERROR', 'message', :MESSAGE, 'SQLCODE', SQLCODE, 'SQLERRM', SQLERRM, 'SQLSTATE', SQLSTATE);
    END;
