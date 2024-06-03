/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

/**
 * In memory implementation of {@link TaskReactorExistenceVerifier}, with {@link
 * #isTaskReactorConfigured()} always returning {@code true}.
 */
public class InMemoryConfiguredTaskReactorExistenceVerifier
    implements TaskReactorExistenceVerifier {

  @Override
  public boolean isTaskReactorConfigured() {
    return true;
  }
}
