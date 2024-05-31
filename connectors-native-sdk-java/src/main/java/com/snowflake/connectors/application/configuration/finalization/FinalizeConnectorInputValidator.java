/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * A validator for the input of {@link FinalizeConnectorHandler}, may be used to provide custom
 * configuration validation.
 *
 * <p>Default implementation of this validator calls the {@code
 * PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION_VALIDATE} procedure.
 */
@FunctionalInterface
public interface FinalizeConnectorInputValidator {

  /**
   * Validates the provided configuration.
   *
   * @param configuration custom configuration provided to the handler
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate(Variant configuration);
}
