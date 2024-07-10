/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link PostDisableResourceCallback}. It calls POST_DISABLE_RESOURCE
 * procedure.
 */
class DefaultPostDisableResourceCallback implements PostDisableResourceCallback {

  private final Session session;

  DefaultPostDisableResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(String resourceIngestionDefinitionId) {
    return callPublicProcedure(
        session, "POST_DISABLE_RESOURCE", asVarchar(resourceIngestionDefinitionId));
  }
}
