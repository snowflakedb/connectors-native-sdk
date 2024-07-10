/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link PreEnableResourceCallback}. It calls PRE_ENABLE_RESOURCE
 * procedure.
 */
class DefaultPreEnableResourceCallback implements PreEnableResourceCallback {

  private final Session session;

  DefaultPreEnableResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(String resourceIngestionDefinitionId) {
    return callPublicProcedure(
        session, "PRE_ENABLE_RESOURCE", asVarchar(resourceIngestionDefinitionId));
  }
}
