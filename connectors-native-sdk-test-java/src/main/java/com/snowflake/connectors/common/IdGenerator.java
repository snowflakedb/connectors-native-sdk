/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common;

import java.util.UUID;

/** Random UUID generator. */
public class IdGenerator {

  /**
   * Generates a random UUID.
   *
   * @return random UUID as String
   */
  public static String randomId() {
    return UUID.randomUUID().toString();
  }
}
