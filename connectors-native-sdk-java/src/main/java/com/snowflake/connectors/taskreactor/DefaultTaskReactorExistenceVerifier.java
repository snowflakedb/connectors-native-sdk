/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.taskreactor.ComponentNames.TASK_REACTOR_SCHEMA;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;

import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link TaskReactorExistenceVerifier}. */
class DefaultTaskReactorExistenceVerifier implements TaskReactorExistenceVerifier {

  private final Session session;

  DefaultTaskReactorExistenceVerifier(Session session) {
    this.session = session;
  }

  @Override
  public boolean isTaskReactorConfigured() {
    return session
        .sql(String.format("SHOW SCHEMAS LIKE %s", asVarchar(TASK_REACTOR_SCHEMA)))
        .first()
        .isPresent();
  }
}
