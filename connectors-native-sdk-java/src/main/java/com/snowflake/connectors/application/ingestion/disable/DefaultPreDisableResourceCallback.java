/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link PreDisableResourceCallback}. It calls PRE_DISABLE_RESOURCE
 * procedure.
 */
class DefaultPreDisableResourceCallback implements PreDisableResourceCallback {

  private final Session session;

  DefaultPreDisableResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(String resourceIngestionDefinitionId) {
    return callPublicProcedure(
        session, "PRE_DISABLE_RESOURCE", asVarchar(resourceIngestionDefinitionId));
  }
}
