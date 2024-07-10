/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;

import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link PreCreateResourceCallback}. It calls PRE_CREATE_RESOURCE
 * procedure.
 */
class DefaultPreCreateResourceCallback implements PreCreateResourceCallback {

  private final Session session;

  DefaultPreCreateResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(VariantResource resource) {
    return callPublicProcedure(session, "PRE_CREATE_RESOURCE", asVariant(mapToVariant(resource)));
  }
}
