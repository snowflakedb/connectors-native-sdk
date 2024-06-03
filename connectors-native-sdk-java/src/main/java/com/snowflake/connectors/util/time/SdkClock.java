/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.time;

import java.time.Instant;

/** SDK abstraction for the {@link java.time.Clock Java Clock}. */
public interface SdkClock {

  /**
   * Returns the current UTC instant.
   *
   * @return current UTC instant
   */
  Instant utcNow();
}
