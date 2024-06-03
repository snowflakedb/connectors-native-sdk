/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

/**
 * In memory implementation of {@link TaskReactorExistenceVerifier}, with {@link
 * #isTaskReactorConfigured()} always returning {@code false}.
 */
public class InMemoryNotConfiguredTaskReactorExistenceVerifier
    implements TaskReactorExistenceVerifier {

  @Override
  public boolean isTaskReactorConfigured() {
    return false;
  }
}
