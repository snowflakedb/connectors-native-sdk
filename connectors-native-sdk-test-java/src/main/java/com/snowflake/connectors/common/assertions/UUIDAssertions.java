/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.UUID;

/** AssertJ based assertions for {@link UUID}. */
public class UUIDAssertions {

  /**
   * Asserts that the provided string is a valid UUID.
   *
   * @param actual asserted value
   */
  public static void assertIsUUID(String actual) {
    assertThat(actual).isNotEmpty();
    assertThatNoException().isThrownBy(() -> UUID.fromString(actual));
    assertThat(UUID.fromString(actual).toString()).isEqualTo(actual);
  }
}
