/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.variant;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when an error during {@link com.snowflake.snowpark_java.types.Variant Variant}
 * mapping is encountered.
 */
public class VariantMapperException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "VARIANT_MAPPING_ERROR";

  /**
   * Creates a new {@link VariantMapperException}.
   *
   * @param message exception message
   * @param cause exception cause
   */
  public VariantMapperException(String message, Throwable cause) {
    super(RESPONSE_CODE, message, cause);
  }
}
