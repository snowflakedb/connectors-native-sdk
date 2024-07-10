/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import java.util.function.Function;

/** In memory implementation of {@link TransactionManager} */
public class InMemoryTransactionManager implements TransactionManager {

  @Override
  public void withTransaction(Runnable action) {
    action.run();
  }

  @Override
  public void withTransaction(
      Runnable action, Function<Throwable, RuntimeException> exceptionMapper) {
    try {
      action.run();
    } catch (Exception e) {
      throw exceptionMapper.apply(e);
    }
  }
}
