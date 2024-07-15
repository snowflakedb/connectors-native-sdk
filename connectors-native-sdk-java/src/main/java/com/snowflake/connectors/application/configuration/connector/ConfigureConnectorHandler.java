/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.common.response.ConnectorResponse.success;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the connector configuration process. A new instance of the handler must be created
 * using {@link #builder(Session) the builder}.
 */
public class ConfigureConnectorHandler {

  /**
   * Error type for the connector configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "CONFIGURE_CONNECTOR_FAILED";

  private final ConfigureConnectorInputValidator inputValidator;
  private final ConfigureConnectorCallback callback;
  private final ConnectorErrorHelper errorHelper;
  private final ConnectorConfigurationService connectorConfigurationService;
  private final ConnectorStatusService connectorStatusService;

  ConfigureConnectorHandler(
      ConfigureConnectorInputValidator inputValidator,
      ConfigureConnectorCallback callback,
      ConnectorErrorHelper errorHelper,
      ConnectorConfigurationService connectorConfigurationService,
      ConnectorStatusService connectorStatusService) {
    this.inputValidator = inputValidator;
    this.callback = callback;
    this.errorHelper = errorHelper;
    this.connectorConfigurationService = connectorConfigurationService;
    this.connectorStatusService = connectorStatusService;
  }

  /**
   * Default handler method for the {@code PUBLIC.CONFIGURE_CONNECTOR} procedure.
   *
   * @param session Snowpark session object
   * @param configuration connector configuration properties
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #configureConnector(Variant) configureConnector}
   */
  public static Variant configureConnector(Session session, Variant configuration) {
    var connectorConfigureHandler = ConfigureConnectorHandler.builder(session).build();
    return connectorConfigureHandler.configureConnector(configuration).toVariant();
  }

  /**
   * Returns a new instance of {@link ConfigureConnectorHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static ConfigureConnectorHandlerBuilder builder(Session session) {
    return new ConfigureConnectorHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of:
   *
   * <ul>
   *   <li>connector status check
   *   <li>{@link ConnectorConfigurationService#validateFields(Variant)}
   *   <li>{@link ConfigureConnectorInputValidator#validate(Variant)}
   *   <li>{@link ConfigureConnectorCallback#execute(Variant)}
   *   <li>connector status update
   * </ul>
   *
   * @param configuration connector configuration properties
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse configureConnector(Variant configuration) {
    return errorHelper.withExceptionLoggingAndWrapping(() -> configureConnectorBody(configuration));
  }

  private ConnectorResponse configureConnectorBody(Variant configuration) {
    var statuses = connectorStatusService.getConnectorStatus();
    validateConnectorStatus(statuses);

    connectorConfigurationService.validateFields(configuration);

    var validationResult = inputValidator.validate(configuration);
    if (validationResult.isNotOk()) {
      return validationResult;
    }

    connectorConfigurationService.updateConfiguration(configuration);

    var internalResult = callback.execute(configuration);
    if (internalResult.isNotOk()) {
      return internalResult;
    }

    updateConnectorStatus(statuses);
    return success("Connector successfully configured.");
  }

  private void validateConnectorStatus(FullConnectorStatus statuses) {
    statuses.validateConnectorStatusIn(ConnectorStatus.CONFIGURING);
  }

  private void updateConnectorStatus(FullConnectorStatus statuses) {
    if (statuses.getConfigurationStatus().isBefore(ConnectorConfigurationStatus.CONFIGURED)) {
      connectorStatusService.updateConnectorStatus(
          new FullConnectorStatus(
              ConnectorStatus.CONFIGURING, ConnectorConfigurationStatus.CONFIGURED));
    }
  }
}
