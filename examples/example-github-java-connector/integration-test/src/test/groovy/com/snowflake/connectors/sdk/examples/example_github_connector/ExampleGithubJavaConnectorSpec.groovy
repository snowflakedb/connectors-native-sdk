package com.snowflake.connectors.sdk.examples.example_github_connector

class ExampleGithubJavaConnectorSpec extends BaseIntegrationSpec {
    def "should enable resource"() {
        given:
        String configStr = """
            {"warehouse": "XSMALL", "destination_database": "$destinationDatabaseName", "secret_name": "${secretsDatabaseName}.PUBLIC.GH_TOKEN", "external_access_integration_name": "$API_INTEGRATION"}
        """
        session.sql("CALL PUBLIC.PROVISION_CONNECTOR(parse_json('$configStr'))").collect()
        session.sql("USE DATABASE $applicationInstanceName").collect()

        and:
        def repository = "apache/airflow"

        when:
        def result = session.sql("CALL PUBLIC.ENABLE_RESOURCE('$repository')").collect()

        then:
        result[0].get(0) == "Resource apache/airflow enabled."
        result.length == 1
    }

    def "should provision connector and ingest data"() {
        given:
        String configStr = """
            {"warehouse": "XSMALL", "destination_database": "$destinationDatabaseName", "secret_name": "${secretsDatabaseName}.PUBLIC.GH_TOKEN", "external_access_integration_name": "$API_INTEGRATION"}
        """
        session.sql("CALL PUBLIC.PROVISION_CONNECTOR(parse_json('$configStr'))").collect()
        session.sql("USE DATABASE $applicationInstanceName").collect()
        def repository = "apache/airflow"

        when:
        def result = session.sql("CALL PUBLIC.INGEST_DATA('$repository')").collect()

        then:
        result[0].get(0).toString() == "\"Stored data to table ${destinationDatabaseName}.DEST_SCHEMA.apache_airflow\""
        result.length == 1

        and:
        def data = session.sql("SELECT * from ${destinationDatabaseName}.DEST_SCHEMA.apache_airflow").collect()
        data.length > 1
    }
}
