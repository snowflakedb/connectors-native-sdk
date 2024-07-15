/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import com.snowflake.snowpark_java.Session;
import java.util.function.Function;

/** Default implementation of {@link TransactionManager} */
class DefaultTransactionManager implements TransactionManager {

  private final Session session;

  DefaultTransactionManager(Session session) {
    this.session = session;
  }

  @Override
  public void withTransaction(Runnable action) {
    withTransaction(action, RuntimeException::new);
  }

  @Override
  public void withTransaction(
      Runnable action, Function<Throwable, RuntimeException> exceptionMapper) {
    session.sql("BEGIN TRANSACTION").collect();
    try {
      action.run();
    } catch (Exception exception) {
      session.sql("ROLLBACK").collect();
      throw exceptionMapper.apply(exception);
    }
    session.sql("COMMIT").collect();
  }
}
