/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import static com.snowflake.connectors.util.snowflake.assertions.Assert.checkThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class AssertTest {

  public static BooleanSupplier falsy = () -> false;
  public static BooleanSupplier truthy = () -> true;

  @Test
  void shouldThrowIllegalArgumentExceptionOnNegativeTest() {

    assertThatThrownBy(
            () -> {
              checkThat(falsy);
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(null);

    assertThatThrownBy(
            () -> {
              checkThat(false);
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(null);

    assertThatThrownBy(
            () -> {
              checkThat(falsy, "bad condition");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("bad condition");

    assertThatThrownBy(
            () -> {
              checkThat(false, "bad condition");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("bad condition");
  }

  @Test
  void shouldNotThrowOnTruthyConditions() {
    assertDoesNotThrow(
        () -> {
          checkThat(truthy);
          checkThat(true);
        });
  }
}
