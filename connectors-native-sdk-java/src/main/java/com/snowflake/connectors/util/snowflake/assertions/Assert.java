/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake.assertions;

import java.util.Optional;
import java.util.function.BooleanSupplier;

/** Utility class to allow for basic sanity checks. */
public class Assert {

  /**
   * Function throws {@link IllegalArgumentException} when given boolean supplier returns false
   *
   * @param supplier boolean supplier check function
   * @param message message to be included in the thrown exception
   * @throws IllegalArgumentException
   */
  public static void checkThat(BooleanSupplier supplier, String message) {
    if (!supplier.getAsBoolean()) {
      Optional.ofNullable(message)
          .ifPresentOrElse(
              (msg) -> {
                throw new IllegalArgumentException(msg);
              },
              () -> {
                throw new IllegalArgumentException();
              });
    }
  }

  /**
   * Function throws {@link IllegalArgumentException} when given boolean supplier returns false
   *
   * @param supplier boolean supplier check function
   * @throws IllegalArgumentException
   */
  public static void checkThat(BooleanSupplier supplier) {
    checkThat(supplier, null);
  }

  /**
   * Function throws {@link IllegalArgumentException} when first boolean argument is false
   *
   * @param value boolean value being asserted
   * @param message message to be included in the thrown exception
   * @throws IllegalArgumentException
   */
  public static void checkThat(boolean value, String message) {
    if (!value) {
      Optional.ofNullable(message)
          .ifPresentOrElse(
              (msg) -> {
                throw new IllegalArgumentException(msg);
              },
              () -> {
                throw new IllegalArgumentException();
              });
    }
  }

  /**
   * Function throws {@link IllegalArgumentException} when given boolean value is false
   *
   * @param value boolean value being checked
   * @throws IllegalArgumentException
   */
  public static void checkThat(boolean value) {
    checkThat(value, null);
  }
}
