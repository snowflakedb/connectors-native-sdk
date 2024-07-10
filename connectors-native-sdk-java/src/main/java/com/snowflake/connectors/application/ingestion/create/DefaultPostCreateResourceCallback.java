/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link PostCreateResourceCallback}. It calls POST_CREATE_RESOURCE
 * procedure.
 */
class DefaultPostCreateResourceCallback implements PostCreateResourceCallback {

  private final Session session;

  DefaultPostCreateResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(String resourceIngestionDefinitionId) {
    return callPublicProcedure(
        session, "POST_CREATE_RESOURCE", asVarchar(resourceIngestionDefinitionId));
  }
}
