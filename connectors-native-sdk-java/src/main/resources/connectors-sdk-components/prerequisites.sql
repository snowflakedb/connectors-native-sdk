-- Copyright (c) 2024 Snowflake Inc.

-- Prerequisites
CREATE TABLE IF NOT EXISTS STATE.PREREQUISITES(
    ID STRING NOT NULL,
    TITLE VARCHAR NOT NULL,
    DESCRIPTION VARCHAR NOT NULL,
    LEARNMORE_URL VARCHAR,
    DOCUMENTATION_URL VARCHAR,
    GUIDE_URL VARCHAR,
    CUSTOM_PROPERTIES VARIANT,
    IS_COMPLETED BOOLEAN DEFAULT FALSE,
    POSITION INTEGER NOT NULL
);

CREATE OR REPLACE VIEW PUBLIC.PREREQUISITES AS
    SELECT
        p.ID,
        p.TITLE,
        p.DESCRIPTION,
        P.LEARNMORE_URL,
        p.DOCUMENTATION_URL,
        P.GUIDE_URL,
        P.CUSTOM_PROPERTIES,
        p.IS_COMPLETED
    FROM STATE.PREREQUISITES p
    ORDER BY POSITION;
GRANT SELECT ON VIEW PUBLIC.PREREQUISITES TO APPLICATION ROLE ADMIN;
GRANT SELECT ON VIEW PUBLIC.PREREQUISITES TO APPLICATION ROLE VIEWER;

CREATE OR REPLACE PROCEDURE PUBLIC.COMPLETE_PREREQUISITES_STEP()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
        AS
            DECLARE
                STATUSES VARIANT DEFAULT (SELECT value FROM STATE.APP_STATE WHERE KEY = 'connector_status');
                CONNECTOR_STATUS VARCHAR DEFAULT (STATUSES:status);
                CONNECTOR_CONFIGURATION_STATUS VARCHAR DEFAULT (STATUSES:configurationStatus);
                INCOMPLETE_PREREQUISITES_NUMBER INTEGER DEFAULT (SELECT COUNT(*) FROM PUBLIC.PREREQUISITES WHERE IS_COMPLETED = FALSE);
                INVALID_CONNECTOR_STATUS_MSG VARCHAR;
                INVALID_CONFIGURATION_STATUS_MSG VARCHAR;
                PREREQUISITES_INCOMPLETE_MSG VARCHAR;
            BEGIN
                --validation of statuses
                IF(:CONNECTOR_STATUS != 'CONFIGURING') THEN
                    INVALID_CONNECTOR_STATUS_MSG := 'Invalid connector status. Expected status: [CONFIGURING]. Current status: ' || :CONNECTOR_STATUS || '.';
                    LET STATEMENT VARCHAR := 'DECLARE INVALID_CONNECTOR_STATUS EXCEPTION(-20001, ''\\\'{"response_code": "INVALID_CONNECTOR_STATUS", "message": "' || :INVALID_CONNECTOR_STATUS_MSG || '"}\\\'''); BEGIN RAISE INVALID_CONNECTOR_STATUS; END;';
                    EXECUTE IMMEDIATE :STATEMENT;
                ELSEIF(:CONNECTOR_CONFIGURATION_STATUS = 'FINALIZED') THEN
                    INVALID_CONFIGURATION_STATUS_MSG := 'Invalid connector configuration status. Expected one of statuses: [INSTALLED, PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: ' || :CONNECTOR_CONFIGURATION_STATUS || '.';
                    LET STATEMENT VARCHAR := 'DECLARE INVALID_CONNECTOR_CONFIGURATION_STATUS EXCEPTION(-20002, ''\\\'{"response_code": "INVALID_CONNECTOR_CONFIGURATION_STATUS", "message": "' || :INVALID_CONFIGURATION_STATUS_MSG || '"}\\\'''); BEGIN RAISE INVALID_CONNECTOR_CONFIGURATION_STATUS; END;';
                    EXECUTE IMMEDIATE :STATEMENT;
                END IF;

                --switch configuration status to PREREQUISITES_DONE if required
                IF(:CONNECTOR_CONFIGURATION_STATUS = 'INSTALLED') THEN
                    UPDATE STATE.APP_STATE SET VALUE =
                        (SELECT PARSE_JSON('{"status": "CONFIGURING", "configurationStatus": "PREREQUISITES_DONE"}'))
                            WHERE KEY = 'connector_status';
                END IF;

                RETURN OBJECT_CONSTRUCT(
                        'response_code', 'OK',
                        'message', 'Prerequisites step completed successfully.'
                    );
            EXCEPTION
                WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
                    CASE
                        WHEN SQLCODE in (-20001) THEN
                            RETURN OBJECT_CONSTRUCT('response_code', 'INVALID_CONNECTOR_STATUS', 'message', :INVALID_CONNECTOR_STATUS_MSG);
                        WHEN SQLCODE in (-20002) THEN
                            RETURN OBJECT_CONSTRUCT('response_code', 'INVALID_CONNECTOR_CONFIGURATION_STATUS', 'message', :INVALID_CONFIGURATION_STATUS_MSG);
                        WHEN SQLCODE in (-20003) THEN
                            RETURN OBJECT_CONSTRUCT('response_code', 'PREREQUISITES_INCOMPLETE', 'message', :PREREQUISITES_INCOMPLETE_MSG);
                        ELSE
                            RAISE;
                    END CASE;
                WHEN OTHER THEN
                    RAISE;
            END;
GRANT USAGE ON PROCEDURE PUBLIC.COMPLETE_PREREQUISITES_STEP() TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_PREREQUISITE(ID VARCHAR, IS_COMPLETED BOOLEAN)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
        AS
            DECLARE
                STATUSES VARIANT DEFAULT (SELECT value FROM STATE.APP_STATE WHERE KEY = 'connector_status');
                CONNECTOR_STATUS VARCHAR DEFAULT (STATUSES:status);
                CONNECTOR_CONFIGURATION_STATUS VARCHAR DEFAULT (STATUSES:configurationStatus);
                PREREQUISITES_WITH_IDENTICAL_ID_NUMBER INTEGER DEFAULT (SELECT COUNT(*) FROM PUBLIC.PREREQUISITES WHERE ID = :ID);
                INVALID_CONNECTOR_STATUS_MSG VARCHAR;
                INVALID_CONFIGURATION_STATUS_MSG VARCHAR;
                PREREQUISITE_NOT_FOUND_MSG VARCHAR;
            BEGIN
                --validation of statuses
                IF(:CONNECTOR_STATUS != 'CONFIGURING') THEN
                    INVALID_CONNECTOR_STATUS_MSG := 'Invalid connector status. Expected status: [CONFIGURING]. Current status: ' || :CONNECTOR_STATUS || '.';
                    LET STATEMENT VARCHAR := 'DECLARE INVALID_CONNECTOR_STATUS EXCEPTION(-20001, ''\\\'{"response_code": "INVALID_CONNECTOR_STATUS", "message": "' || :INVALID_CONNECTOR_STATUS_MSG || '"}\\\'''); BEGIN RAISE INVALID_CONNECTOR_STATUS; END;';
                    EXECUTE IMMEDIATE :STATEMENT;
                ELSEIF(:CONNECTOR_CONFIGURATION_STATUS = 'FINALIZED') THEN
                    INVALID_CONFIGURATION_STATUS_MSG := 'Invalid connector configuration status. Expected one of statuses: [INSTALLED, PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: ' || :CONNECTOR_CONFIGURATION_STATUS || '.';
                    LET STATEMENT VARCHAR := 'DECLARE INVALID_CONNECTOR_CONFIGURATION_STATUS EXCEPTION(-20002, ''\\\'{"response_code": "INVALID_CONNECTOR_CONFIGURATION_STATUS", "message": "' || :INVALID_CONFIGURATION_STATUS_MSG || '"}\\\'''); BEGIN RAISE INVALID_CONNECTOR_CONFIGURATION_STATUS; END;';
                    EXECUTE IMMEDIATE :STATEMENT;
                END IF;

                --validation of whether the updated prerequisite exists
                IF(:PREREQUISITES_WITH_IDENTICAL_ID_NUMBER = 0) THEN
                    PREREQUISITE_NOT_FOUND_MSG := 'Prerequisite with ID: ' || :ID || ' not found.';
                    LET STATEMENT VARCHAR := 'DECLARE PREREQUISITE_NOT_FOUND EXCEPTION(-20003, ''\\\'{"response_code": "PREREQUISITE_NOT_FOUND", "message": "' || :PREREQUISITE_NOT_FOUND_MSG || '"}\\\'''); BEGIN RAISE PREREQUISITE_NOT_FOUND; END;';
                    EXECUTE IMMEDIATE :STATEMENT;
                END IF;

                UPDATE STATE.PREREQUISITES SET IS_COMPLETED = :IS_COMPLETED WHERE ID = :ID;
                RETURN OBJECT_CONSTRUCT(
                        'response_code', 'OK',
                        'message', 'Prerequisite updated successfully.'
                    );
            EXCEPTION
                WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
                    CASE
                        WHEN SQLCODE in (-20001) THEN
                            RETURN OBJECT_CONSTRUCT('response_code', 'INVALID_CONNECTOR_STATUS', 'message', :INVALID_CONNECTOR_STATUS_MSG);
                        WHEN SQLCODE in (-20002) THEN
                            RETURN OBJECT_CONSTRUCT('response_code', 'INVALID_CONNECTOR_CONFIGURATION_STATUS', 'message', :INVALID_CONFIGURATION_STATUS_MSG);
                        WHEN SQLCODE in (-20003) THEN
                            RETURN OBJECT_CONSTRUCT('response_code', 'PREREQUISITE_NOT_FOUND', 'message', :PREREQUISITE_NOT_FOUND_MSG);
                        ELSE
                            RAISE;
                    END CASE;
                WHEN OTHER THEN
                    RAISE;
            END;
GRANT USAGE ON PROCEDURE PUBLIC.UPDATE_PREREQUISITE(VARCHAR, BOOLEAN) TO APPLICATION ROLE ADMIN;

CREATE OR REPLACE PROCEDURE PUBLIC.MARK_ALL_PREREQUISITES_AS_DONE()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
        AS
        DECLARE
            STATUSES VARIANT DEFAULT (SELECT value FROM STATE.APP_STATE WHERE KEY = 'connector_status');
            CONNECTOR_STATUS VARCHAR DEFAULT (STATUSES:status);
            CONNECTOR_CONFIGURATION_STATUS VARCHAR DEFAULT (STATUSES:configurationStatus);
            INVALID_CONNECTOR_STATUS_MSG VARCHAR;
            INVALID_CONFIGURATION_STATUS_MSG VARCHAR;
        BEGIN
            --validation of statuses
            IF(:CONNECTOR_STATUS != 'CONFIGURING') THEN
                INVALID_CONNECTOR_STATUS_MSG := 'Invalid connector status. Expected status: [CONFIGURING]. Current status: ' || :CONNECTOR_STATUS || '.';
                LET STATEMENT VARCHAR := 'DECLARE INVALID_CONNECTOR_STATUS EXCEPTION(-20001, ''\\\'{"response_code": "INVALID_CONNECTOR_STATUS", "message": "' || :INVALID_CONNECTOR_STATUS_MSG || '"}\\\'''); BEGIN RAISE INVALID_CONNECTOR_STATUS; END;';
                EXECUTE IMMEDIATE :STATEMENT;
            ELSEIF(:CONNECTOR_CONFIGURATION_STATUS = 'FINALIZED') THEN
                INVALID_CONFIGURATION_STATUS_MSG := 'Invalid connector configuration status. Expected one of statuses: [INSTALLED, PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: ' || :CONNECTOR_CONFIGURATION_STATUS || '.';
                LET STATEMENT VARCHAR := 'DECLARE INVALID_CONNECTOR_CONFIGURATION_STATUS EXCEPTION(-20002, ''\\\'{"response_code": "INVALID_CONNECTOR_CONFIGURATION_STATUS", "message": "' || :INVALID_CONFIGURATION_STATUS_MSG || '"}\\\'''); BEGIN RAISE INVALID_CONNECTOR_CONFIGURATION_STATUS; END;';
                EXECUTE IMMEDIATE :STATEMENT;
            END IF;

            UPDATE STATE.PREREQUISITES SET IS_COMPLETED = TRUE WHERE IS_COMPLETED = FALSE;
            RETURN OBJECT_CONSTRUCT(
                    'response_code', 'OK',
                    'message', 'All prerequisites have been marked as done.'
                );
        EXCEPTION
            WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
                CASE
                    WHEN SQLCODE in (-20001) THEN
                        RETURN OBJECT_CONSTRUCT('response_code', 'INVALID_CONNECTOR_STATUS', 'message', :INVALID_CONNECTOR_STATUS_MSG);
                    WHEN SQLCODE in (-20002) THEN
                        RETURN OBJECT_CONSTRUCT('response_code', 'INVALID_CONNECTOR_CONFIGURATION_STATUS', 'message', :INVALID_CONFIGURATION_STATUS_MSG);
                    ELSE
                        RAISE;
                END CASE;
            WHEN OTHER THEN
                RAISE;
        END;
GRANT USAGE ON PROCEDURE PUBLIC.MARK_ALL_PREREQUISITES_AS_DONE() TO APPLICATION ROLE ADMIN;
