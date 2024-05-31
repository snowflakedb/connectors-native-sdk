/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.context;

import java.util.HashMap;
import java.util.Map;

/** A mutable implementation of {@link ExceptionContext}. */
public class MutableExceptionContext implements ExceptionContext {

  private final Map<String, String> context = new HashMap<>();

  /**
   * Puts a new key-value pair into the properties of this context.
   *
   * @param key property key
   * @param value property value
   * @return this context
   * @throws IllegalStateException if this context already contains a specified key
   */
  public MutableExceptionContext put(String key, String value) {
    if (context.containsKey(key)) {
      throw new IllegalStateException(
          String.format("Unable to insert key %s with value: %s (Duplicate key)", key, value));
    }

    context.put(key, value);
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @return an immutable map containing properties of this context
   */
  @Override
  public Map<String, Object> asMap() {
    return Map.copyOf(context);
  }
}
