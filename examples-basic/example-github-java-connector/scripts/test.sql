use role accountadmin;
use database &{APP_INSTANCE_NAME};

call &{APP_INSTANCE_NAME}.public.provision_connector(
    parse_json('{"warehouse": "xs", "destination_database": "github.issues", "secret_name": "{{ user }}_sdk.public.github_token", "external_access_integration_name": "gh_integration"}')
);
call &{APP_INSTANCE_NAME}.public.ingest_data('apache/airflow');

select * from github.issues.apache_airflow;
