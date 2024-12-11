/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the reset configuration process. A new instance of the handler must be created using
 * {@link #builder(Session) the builder}.
 */
public class ResetConfigurationHandler {

  /** Error type for the reset configuration failure, used by the {@link ConnectorErrorHelper}. */
  public static final String ERROR_TYPE = "RESET_CONFIGURATION_FAILED";

  private final ResetConfigurationValidator validator;
  private final ResetConfigurationSdkCallback sdkCallback;
  private final ResetConfigurationCallback callback;
  private final ConnectorErrorHelper errorHelper;
  private final ConnectorStatusService connectorStatusService;

  ResetConfigurationHandler(
      ResetConfigurationValidator validator,
      ResetConfigurationSdkCallback sdkCallback,
      ResetConfigurationCallback callback,
      ConnectorErrorHelper errorHelper,
      ConnectorStatusService connectorStatusService) {
    this.validator = validator;
    this.sdkCallback = sdkCallback;
    this.callback = callback;
    this.errorHelper = errorHelper;
    this.connectorStatusService = connectorStatusService;
  }

  /**
   * Default handler method for the {@code PUBLIC.RESET_CONFIGURATION} procedure.
   *
   * @param session Snowpark session object
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #resetConfiguration() resetConfiguration}
   */
  public static Variant resetConfiguration(Session session) {
    var resetConfigurationHandler = ResetConfigurationHandler.builder(session).build();
    return resetConfigurationHandler.resetConfiguration().toVariant();
  }

  /**
   * Returns a new instance of {@link ResetConfigurationHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static ResetConfigurationHandlerBuilder builder(Session session) {
    return new ResetConfigurationHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of:
   *
   * <ul>
   *   <li>connector status check
   *   <li>{@link ResetConfigurationValidator#validate()}
   *   <li>{@link ResetConfigurationCallback#execute()}
   *   <li>{@link ResetConfigurationSdkCallback#execute()}
   *   <li>connector status update
   * </ul>
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse resetConfiguration() {
    return errorHelper.withExceptionLogging(this::resetConfigurationBody);
  }

  private ConnectorResponse resetConfigurationBody() {
    validateConnectorStatus();

    var validateResponse = validator.validate();
    if (validateResponse.isNotOk()) {
      return validateResponse;
    }

    var callbackResponse = callback.execute();
    if (callbackResponse.isNotOk()) {
      return callbackResponse;
    }

    var sdkCallbackResponse = this.sdkCallback.execute();
    if (sdkCallbackResponse.isNotOk()) {
      return sdkCallbackResponse;
    }

    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(CONFIGURING, ConnectorConfigurationStatus.INSTALLED));

    return ConnectorResponse.success();
  }

  private void validateConnectorStatus() {
    FullConnectorStatus actualConnectorState = this.connectorStatusService.getConnectorStatus();
    actualConnectorState.validateConnectorStatusIn(CONFIGURING);
    actualConnectorState.validateConfigurationStatusIsIn(
        ConnectorConfigurationStatus.INSTALLED,
        ConnectorConfigurationStatus.PREREQUISITES_DONE,
        ConnectorConfigurationStatus.CONFIGURED,
        ConnectorConfigurationStatus.CONNECTED);
  }
}
