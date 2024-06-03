/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception.helper;

import com.snowflake.connectors.common.exception.InvalidInputException;
import com.snowflake.snowpark_java.types.Variant;

/** Validator for input parameters in handlers. */
public class InputValidator {

  /**
   * Validates whether given input parameter is valid.
   *
   * @param input variant given as input parameter for handler.
   * @param paramName name of the input parameter
   */
  public static void requireNonNull(Variant input, String paramName) {
    if (input == null) {
      throw new InvalidInputException(
          String.format("Input parameter '%s' is required and cannot be null", paramName));
    }
  }
}
