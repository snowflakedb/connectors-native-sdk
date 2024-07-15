/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Handler for the connection configuration update process. A new instance of the handler must be
 * created using {@link #builder(Session) the builder}.
 */
public class UpdateConnectionConfigurationHandler {

  /**
   * Error type for the connection configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "UPDATE_CONNECTION_CONFIGURATION_FAILED";

  private final ConnectionConfigurationInputValidator inputValidator;
  private final ConnectionConfigurationCallback draftCallback;
  private final DraftConnectionValidator draftConnectionValidator;
  private final ConnectionConfigurationCallback callback;
  private final ConnectionValidator connectionValidator;
  private final ConnectorErrorHelper errorHelper;
  private final ConnectionConfigurationService connectionConfigurationService;
  private final ConnectorStatusService connectorStatusService;

  UpdateConnectionConfigurationHandler(
      ConnectionConfigurationInputValidator inputValidator,
      ConnectionConfigurationCallback draftCallback,
      DraftConnectionValidator draftConnectionValidator,
      ConnectionConfigurationCallback callback,
      ConnectionValidator connectionValidator,
      ConnectorErrorHelper errorHelper,
      ConnectionConfigurationService connectionConfigurationService,
      ConnectorStatusService connectorStatusService) {
    this.inputValidator = inputValidator;
    this.draftCallback = draftCallback;
    this.draftConnectionValidator = draftConnectionValidator;
    this.callback = callback;
    this.connectionValidator = connectionValidator;
    this.errorHelper = errorHelper;
    this.connectionConfigurationService = connectionConfigurationService;
    this.connectorStatusService = connectorStatusService;
  }

  /**
   * Default handler method for the {@code PUBLIC.UPDATE_CONNECTION_CONFIGURATION} procedure.
   *
   * @param session Snowpark session object
   * @param configuration connection configuration properties
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #updateConnectionConfiguration(Variant) setConnectionConfiguration}
   */
  public static Variant updateConnectionConfiguration(Session session, Variant configuration) {
    var handler = UpdateConnectionConfigurationHandler.builder(session).build();
    return handler.updateConnectionConfiguration(configuration).toVariant();
  }

  /**
   * Returns a new instance of {@link ConnectionConfigurationHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static UpdateConnectionConfigurationHandlerBuilder builder(Session session) {
    return new UpdateConnectionConfigurationHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of:
   *
   * <ul>
   *   <li>connector status check
   *   <li>{@link ConnectionConfigurationInputValidator#validate(Variant)}
   *   <li>{@link ConnectionConfigurationCallback#execute(Variant)} for a draft check
   *   <li>{@link DraftConnectionValidator#validate(Variant)}
   *   <li>{@link ConnectionConfigurationCallback#execute(Variant)}
   *   <li>{@link ConnectionValidator#validate()}
   *   <li>connector status update
   * </ul>
   *
   * @param configuration connection configuration properties
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse updateConnectionConfiguration(Variant configuration) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> updateConnectionConfigurationBody(configuration));
  }

  private ConnectorResponse updateConnectionConfigurationBody(Variant config) {
    validateConnectorStatus();

    var inputValidationResponse = inputValidator.validate(config);
    if (inputValidationResponse.isNotOk()) {
      return inputValidationResponse;
    }

    var updatedConfig =
        Optional.ofNullable(inputValidationResponse.getAdditionalPayload().get("config"))
            .orElse(config);

    var draftCallbackResponse = draftCallback.execute(updatedConfig);
    if (draftCallbackResponse.isNotOk()) {
      return draftCallbackResponse;
    }

    var draftConnectionValidationResponse = draftConnectionValidator.validate(updatedConfig);
    if (draftConnectionValidationResponse.isNotOk()) {
      return draftConnectionValidationResponse;
    }

    connectionConfigurationService.updateConfiguration(updatedConfig);

    var callbackResponse = callback.execute(updatedConfig);
    if (callbackResponse.isNotOk()) {
      return callbackResponse;
    }

    return connectionValidator.validate();
  }

  private void validateConnectorStatus() {
    var statuses = connectorStatusService.getConnectorStatus();
    statuses.validateConnectorStatusIn(PAUSED);
  }
}
