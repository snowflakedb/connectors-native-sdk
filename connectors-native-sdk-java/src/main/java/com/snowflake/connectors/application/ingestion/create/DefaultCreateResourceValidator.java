/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;

import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link CreateResourceValidator}. It calls CREATE_RESOURCE_VALIDATE
 * procedure.
 */
class DefaultCreateResourceValidator implements CreateResourceValidator {

  private final Session session;

  DefaultCreateResourceValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate(VariantResource resource) {
    return callPublicProcedure(
        session, "CREATE_RESOURCE_VALIDATE", asVariant(mapToVariant(resource)));
  }
}
