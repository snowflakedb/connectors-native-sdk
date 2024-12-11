/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake.assertions;

import java.util.Optional;
import java.util.function.BooleanSupplier;

/** Utility class to allow for basic sanity checks. */
public class Assert {

  /**
   * Throws an exception when the given boolean supplier returns false.
   *
   * @param supplier boolean supplier check function
   * @param message message to be included in the thrown exception
   * @throws IllegalArgumentException when the given boolean supplier returns false
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
   * Throws an exception when the given boolean supplier returns false.
   *
   * @param supplier boolean supplier check function
   * @throws IllegalArgumentException when the given boolean supplier returns false
   */
  public static void checkThat(BooleanSupplier supplier) {
    checkThat(supplier, null);
  }

  /**
   * Throws an exception when the provided boolean is false
   *
   * @param value boolean value being checked
   * @param message message to be included in the thrown exception
   * @throws IllegalArgumentException when the given boolean is false
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
   * Throws an exception when the provided boolean is false
   *
   * @param value boolean value being checked
   * @throws IllegalArgumentException when the given boolean is false
   */
  public static void checkThat(boolean value) {
    checkThat(value, null);
  }
}
