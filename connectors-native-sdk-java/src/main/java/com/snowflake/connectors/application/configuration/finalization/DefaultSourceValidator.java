/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.sql.SqlTools.variantArgument;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link SourceValidator}. */
class DefaultSourceValidator implements SourceValidator {

  private final Session session;

  DefaultSourceValidator(Session session) {
    this.session = session;
  }

  public ConnectorResponse validate(Variant configuration) {
    return callPublicProcedure(session, "VALIDATE_SOURCE", variantArgument(configuration));
  }
}
