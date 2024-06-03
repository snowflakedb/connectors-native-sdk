/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * A validator for the draft connection configured during {@link
 * UpdateConnectionConfigurationHandler} execution. Should be used to check if any connection to the
 * external service can be established.
 *
 * <p>Default implementation of this validator calls the {@code PUBLIC.TEST_DRAFT_CONNECTION}
 * procedure.
 *
 * <p>Additional SQL grants (e.g. to the external access integration) may be needed before
 * connection validation.
 */
@FunctionalInterface
public interface DraftConnectionValidator {

  /**
   * Validates the connection configured during the handler execution.
   *
   * @param configuration connection configuration provided to the handler that should be used in
   *     draft connection.
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate(Variant configuration);
}
