/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static com.snowflake.connectors.example.ConnectorObjects.FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.PUBLIC_SCHEMA;
import static com.snowflake.connectors.example.ConnectorObjects.SETUP_EXTERNAL_INTEGRATION_WITH_NAMES_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.SETUP_EXTERNAL_INTEGRATION_WITH_REFS_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.TEST_CONNECTION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.WORKER_PROCEDURE;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callProcedure;
import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link ConnectionConfigurationCallback}, used by the {@link
 * TemplateConnectionConfigurationHandler}, providing external access configuration for the
 * connector procedures.
 */
public class TemplateConnectionConfigurationCallback implements ConnectionConfigurationCallback {

  private static final String[] EXTERNAL_SOURCE_PROCEDURE_SIGNATURES = {
    asVarchar(format("%s.%s()", PUBLIC_SCHEMA, TEST_CONNECTION_PROCEDURE)),
    asVarchar(format("%s.%s(VARIANT)", PUBLIC_SCHEMA, FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE)),
    asVarchar(format("%s.%s(NUMBER, STRING)", PUBLIC_SCHEMA, WORKER_PROCEDURE))
  };
  private final Session session;

  public TemplateConnectionConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant config) {
    // TODO: If you need to alter some procedures with external access you can use
    //  configureProcedure method or implement a similar method on your own.

    // TODO: IMPLEMENT ME connection callback: Implement the custom logic of changes in application
    //  to be done after connection configuration, like altering procedures with external access.
    //  See more in docs:
    //
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connection_configuration_reference
    //  https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connection_configuration

    // TODO: uncomment below code and choose the procedure altering method according to the
    //  way of managing EAI and SECRET objects in your connector
    //    var response = configureProceduresWithNames();
    //    var response = configureProceduresWithReferences();
    //    if (response.isNotOk()) {
    //      return response;
    //    }
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME connection callback'");
  }

  private ConnectorResponse configureProceduresWithNames() {
    return callProcedure(
        session,
        PUBLIC_SCHEMA,
        SETUP_EXTERNAL_INTEGRATION_WITH_REFS_PROCEDURE,
        EXTERNAL_SOURCE_PROCEDURE_SIGNATURES);
  }

  private ConnectorResponse configureProceduresWithReferences() {
    return callProcedure(
        session,
        PUBLIC_SCHEMA,
        SETUP_EXTERNAL_INTEGRATION_WITH_NAMES_PROCEDURE,
        EXTERNAL_SOURCE_PROCEDURE_SIGNATURES);
  }
}
