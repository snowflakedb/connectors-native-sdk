/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.sql.SqlTools.variantArgument;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link DraftConnectionValidator}. */
class TestDraftConnectionValidator implements DraftConnectionValidator {

  private final Session session;

  TestDraftConnectionValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate(Variant configuration) {
    return callPublicProcedure(session, "TEST_DRAFT_CONNECTION", variantArgument(configuration));
  }
}
