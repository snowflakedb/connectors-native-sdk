/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * A validator for the connection to the external service, used by {@link FinalizeConnectorHandler}.
 * Should be used to check if access to specific external sources can be established.
 *
 * <p>Default implementation of this validator calls the {@code PUBLIC.VALIDATE_SOURCE} procedure.
 *
 * <p>Additional SQL grants (e.g. to the external access integration) may be needed before source
 * validation.
 */
@FunctionalInterface
public interface SourceValidator {

  /**
   * Validates the access to specific external sources.
   *
   * @param configuration custom configuration provided to the handler
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate(Variant configuration);
}
