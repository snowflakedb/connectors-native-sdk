/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link PostEnableResourceCallback}. It calls POST_ENABLE_RESOURCE
 * procedure.
 */
class DefaultPostEnableResourceCallback implements PostEnableResourceCallback {

  private final Session session;

  DefaultPostEnableResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(String resourceIngestionDefinitionId) {
    return callPublicProcedure(
        session, "POST_ENABLE_RESOURCE", asVarchar(resourceIngestionDefinitionId));
  }
}
