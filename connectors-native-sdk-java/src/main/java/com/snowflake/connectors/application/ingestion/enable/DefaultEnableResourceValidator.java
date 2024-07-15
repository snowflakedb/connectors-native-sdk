/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/**
 * Default implementation of {@link EnableResourceValidator}. It calls ENABLE_RESOURCE_VALIDATE
 * procedure.
 */
class DefaultEnableResourceValidator implements EnableResourceValidator {

  private final Session session;

  DefaultEnableResourceValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate(String resourceIngestionDefinitionId) {
    return callPublicProcedure(
        session, "ENABLE_RESOURCE_VALIDATE", asVarchar(resourceIngestionDefinitionId));
  }
}
