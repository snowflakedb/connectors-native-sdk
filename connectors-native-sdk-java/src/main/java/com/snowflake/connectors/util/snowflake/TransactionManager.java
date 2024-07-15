/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import com.snowflake.snowpark_java.Session;
import java.util.function.Function;

/** Wraps given action with transaction. If an exception is thrown, then rollback is executed. */
public interface TransactionManager {

  /**
   * Wraps given action with transaction. If an exception is thrown, then rollback is executed. The
   * exception is rethrown after rollback.
   *
   * @param action action to be executed
   */
  void withTransaction(Runnable action);

  /**
   * Wraps given action with transaction. If an exception is thrown, then rollback is executed.
   * After rollback, the exception is mapped into a custom exception using exceptionMapper and
   * rethrown.
   *
   * @param action action to be executed
   * @param exceptionMapper function that maps caught exception into custom exception
   */
  void withTransaction(Runnable action, Function<Throwable, RuntimeException> exceptionMapper);

  /**
   * Creates new instance of Transaction Manager
   *
   * @param session snowpark session
   * @return new instance
   */
  static TransactionManager getInstance(Session session) {
    return new DefaultTransactionManager(session);
  }
}
