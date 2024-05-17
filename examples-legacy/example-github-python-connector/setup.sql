create or alter versioned schema public;
create schema if not exists state;
create schema if not exists tasks;

create application role app_user;
grant usage on schema public to application role app_user;
grant usage on schema tasks to application role app_user;

-- Tables
create table state.app_configuration(
    key string,
    value variant
);
create table state.resource_configuration(
    key string,
    value variant
);
create table state.app_state(
    timestamp timestamp_ntz default SYSDATE(),
    key string,
    value variant
);

-- Procedures
create or replace procedure public.enable_resource(resource_id string)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests')
imports = ('/snowflake_github_connector.zip')
handler='snowflake_github_connector.procedures.enable_resource'
;
grant usage on procedure public.enable_resource(string) to application role app_user;


create or replace procedure public.provision_connector(config variant)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests')
imports = ('/snowflake_github_connector.zip')
handler='snowflake_github_connector.procedures.provision_connector'
;
grant usage on procedure public.provision_connector(variant) to application role app_user;


create or replace procedure public.ingest_data(resource_id string)
returns variant
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests')
imports = ('/snowflake_github_connector.zip')
handler='snowflake_github_connector.procedures.ingest_data'
;
grant usage on procedure public.ingest_data(string) to application role app_user;



create or replace streamlit public.github_app from  '/'
main_file = 'streamlit_app.py';

grant usage on streamlit public.github_app to application role app_user;

CREATE PROCEDURE PUBLIC.REGISTER_REFERENCE(ref_name STRING, operation STRING, ref_or_alias STRING)
    RETURNS STRING
    LANGUAGE SQL
AS $$
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
$$;

GRANT USAGE ON PROCEDURE PUBLIC.REGISTER_REFERENCE(STRING, STRING, STRING)
  TO APPLICATION ROLE app_user;
