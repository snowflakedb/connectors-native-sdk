/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * Default implementation of {@link PostUpdateResourceCallback}. It calls POST_UPDATE_RESOURCE
 * procedure.
 */
class DefaultPostUpdateResourceCallback implements PostUpdateResourceCallback {

  private final Session session;

  DefaultPostUpdateResourceCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    return callPublicProcedure(
        session,
        "POST_UPDATE_RESOURCE",
        asVarchar(resourceIngestionDefinitionId),
        asVariant(mapToVariant(ingestionConfigurations)));
  }
}
