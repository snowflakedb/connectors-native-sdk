/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.time;

import java.time.Clock;
import java.time.Instant;

/**
 * Default implementation of {@link SdkClock}, using the system {@link Clock#systemUTC() UTC clock}.
 */
public class SystemClock implements SdkClock {

  private final Clock internalClock;

  /** Creates a new {@link SystemClock}. */
  public SystemClock() {
    this.internalClock = Clock.systemUTC();
  }

  @Override
  public Instant utcNow() {
    return Instant.now(internalClock);
  }
}
