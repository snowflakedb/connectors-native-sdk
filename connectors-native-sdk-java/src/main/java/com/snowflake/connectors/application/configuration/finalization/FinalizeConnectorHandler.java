/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the configuration finalization process. A new instance of the handler must be created
 * using {@link #builder(Session) the builder}.
 */
public class FinalizeConnectorHandler {

  /**
   * Error type for the connector configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  static final String ERROR_TYPE = "FINALIZE_CONNECTOR_CONFIGURATION_FAILED";

  private final FinalizeConnectorInputValidator inputValidator;
  private final SourceValidator sourceValidator;
  private final FinalizeConnectorCallback callback;
  private final ConnectorErrorHelper errorHelper;
  private final ConnectorStatusService connectorStatusService;
  private final FinalizeConnectorSdkCallback sdkCallback;

  FinalizeConnectorHandler(
      FinalizeConnectorInputValidator inputValidator,
      SourceValidator sourceValidator,
      FinalizeConnectorCallback callback,
      ConnectorErrorHelper errorHelper,
      ConnectorStatusService connectorStatusService,
      FinalizeConnectorSdkCallback sdkCallback) {
    this.inputValidator = inputValidator;
    this.sourceValidator = sourceValidator;
    this.callback = callback;
    this.errorHelper = errorHelper;
    this.connectorStatusService = connectorStatusService;
    this.sdkCallback = sdkCallback;
  }

  /**
   * Default handler method for the {@code PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION} procedure.
   *
   * @param session Snowpark session object
   * @param configuration custom configuration properties
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #finalizeConnectorConfiguration(Variant) finalizeConnectorConfiguration}
   */
  public static Variant finalizeConnectorConfiguration(Session session, Variant configuration) {
    var handler = builder(session).build();
    return handler.finalizeConnectorConfiguration(configuration).toVariant();
  }

  /**
   * Returns a new instance of {@link FinalizeConnectorHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static FinalizeConnectorHandlerBuilder builder(Session session) {
    return new FinalizeConnectorHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of:
   *
   * <ul>
   *   <li>connector status check
   *   <li>{@link FinalizeConnectorInputValidator#validate(Variant)}
   *   <li>{@link SourceValidator#validate(Variant)}
   *   <li>{@link FinalizeConnectorCallback#execute(Variant)}
   *   <li>connector status update
   * </ul>
   *
   * <p>Be aware - this handler does not persist any provided configuration, any such logic has to
   * be implemented using a custom {@link FinalizeConnectorCallback}.
   *
   * @param configuration custom configuration properties
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse finalizeConnectorConfiguration(Variant configuration) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> finalizeConnectorConfigurationBody(configuration));
  }

  private ConnectorResponse finalizeConnectorConfigurationBody(Variant customConfiguration) {
    validateConnectorStatus();

    var validateResponse = inputValidator.validate(customConfiguration);
    if (validateResponse.isNotOk()) {
      return validateResponse;
    }

    var validateSourceResponse = sourceValidator.validate(customConfiguration);
    if (validateSourceResponse.isNotOk()) {
      return validateSourceResponse;
    }

    var internalResponse = callback.execute(customConfiguration);
    if (internalResponse.isNotOk()) {
      return internalResponse;
    }

    var sdkCallbackResponse = sdkCallback.execute();
    if (sdkCallbackResponse.isNotOk()) {
      return sdkCallbackResponse;
    }

    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));
    return internalResponse;
  }

  private void validateConnectorStatus() {
    var actualConnectorState = connectorStatusService.getConnectorStatus();
    actualConnectorState.validateConnectorStatusIn(CONFIGURING);
    actualConnectorState.validateConfigurationStatusIsIn(CONNECTED);
  }
}
