/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.context;

import java.util.Collections;
import java.util.Map;

/** Empty implementation of {@link ExceptionContext}. */
public final class EmptyExceptionContext implements ExceptionContext {

  /** An instance of the {@link EmptyExceptionContext}. */
  public static final EmptyExceptionContext INSTANCE = new EmptyExceptionContext();

  private EmptyExceptionContext() {}

  /**
   * {@inheritDoc}
   *
   * @return an empty immutable map
   */
  @Override
  public Map<String, Object> asMap() {
    return Collections.emptyMap();
  }
}
