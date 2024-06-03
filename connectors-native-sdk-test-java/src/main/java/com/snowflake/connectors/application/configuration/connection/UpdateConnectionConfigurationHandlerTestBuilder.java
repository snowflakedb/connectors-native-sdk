/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link UpdateConnectionConfigurationHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectionConfigurationInputValidator}
 *   <li>{@link ConnectionConfigurationCallback}
 *   <li>{@link DraftConnectionValidator}
 *   <li>{@link ConnectionConfigurationCallback}
 *   <li>{@link ConnectionValidator}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ConnectionConfigurationService}
 *   <li>{@link ConnectorStatusService}
 * </ul>
 */
public class UpdateConnectionConfigurationHandlerTestBuilder {

  private ConnectionConfigurationInputValidator inputValidator;
  private ConnectionConfigurationCallback draftCallback;
  private DraftConnectionValidator draftConnectionValidator;
  private ConnectionConfigurationCallback callback;
  private ConnectionValidator connectionValidator;
  private ConnectorErrorHelper errorHelper;
  private ConnectionConfigurationService connectionConfigurationService;
  private ConnectorStatusService connectorStatusService;

  /**
   * Creates a new, empty {@link UpdateConnectionConfigurationHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public UpdateConnectionConfigurationHandlerTestBuilder() {}

  /**
   * Creates a new {@link UpdateConnectionConfigurationHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ConnectionConfigurationInputValidator}
   *   <li>a draft implementation of {@link ConnectionConfigurationCallback}
   *   <li>a default implementation of {@link DraftConnectionValidator}
   *   <li>a default implementation of {@link ConnectionConfigurationCallback}
   *   <li>a default implementation of {@link ConnectionValidator}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ConnectionConfigurationService}
   *   <li>a default implementation of {@link ConnectorStatusService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  UpdateConnectionConfigurationHandlerTestBuilder(Session session) {
    requireNonNull(session);

    this.inputValidator = new DefaultConnectionConfigurationInputValidator(session);
    this.draftCallback = new DraftConnectionConfigurationCallback(session);
    this.draftConnectionValidator = new TestDraftConnectionValidator(session);
    this.callback = new InternalConnectionConfigurationCallback(session);
    this.connectionValidator = new TestConnectionValidator(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, ConnectionConfigurationHandler.ERROR_TYPE);
    this.connectionConfigurationService = ConnectionConfigurationService.getInstance(session);
    this.connectorStatusService = ConnectorStatusService.getInstance(session);
  }

  /**
   * @param inputValidator implementation of {@link ConnectionConfigurationInputValidator} interface
   *     that will be used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withInputValidator(
      ConnectionConfigurationInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    return this;
  }

  /**
   * @param draftCallback implementation of {@link ConnectionConfigurationCallback} interface that
   *     will be used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withDraftCallback(
      ConnectionConfigurationCallback draftCallback) {
    this.draftCallback = draftCallback;
    return this;
  }

  /**
   * @param draftConnectionValidator implementation of {@link DraftConnectionValidator} interface
   *     that will be used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withDraftConnectionValidator(
      DraftConnectionValidator draftConnectionValidator) {
    this.draftConnectionValidator = draftConnectionValidator;
    return this;
  }

  /**
   * @param callback implementation of {@link ConnectionConfigurationCallback} interface that will
   *     be used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withCallback(
      ConnectionConfigurationCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * @param connectionValidator implementation of {@link ConnectionValidator} interface that will be
   *     used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withConnectionValidator(
      ConnectionValidator connectionValidator) {
    this.connectionValidator = connectionValidator;
    return this;
  }

  /**
   * @param errorHelper implementation of {@link ConnectorErrorHelper} interface that will be used
   *     in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withErrorHelper(
      ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * @param connectionConfigurationService implementation of {@link ConnectionConfigurationService}
   *     interface that will be used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withConnectionConfigurationService(
      ConnectionConfigurationService connectionConfigurationService) {
    this.connectionConfigurationService = connectionConfigurationService;
    return this;
  }

  /**
   * @param connectorStatusService implementation of {@link ConnectorStatusService} interface that
   *     will be used in built UpdateConnectionConfigurationHandler
   * @return this builder instance with updated attribute
   */
  public UpdateConnectionConfigurationHandlerTestBuilder withConnectorStatusService(
      ConnectorStatusService connectorStatusService) {
    this.connectorStatusService = connectorStatusService;
    return this;
  }

  /**
   * @return an instance of {@link UpdateConnectionConfigurationHandler} with customised services
   */
  public UpdateConnectionConfigurationHandler build() {
    requireNonNull(inputValidator);
    requireNonNull(draftCallback);
    requireNonNull(draftConnectionValidator);
    requireNonNull(callback);
    requireNonNull(connectionValidator);
    requireNonNull(errorHelper);
    requireNonNull(connectionConfigurationService);
    requireNonNull(connectorStatusService);

    return new UpdateConnectionConfigurationHandler(
        inputValidator,
        draftCallback,
        draftConnectionValidator,
        callback,
        connectionValidator,
        errorHelper,
        connectionConfigurationService,
        connectorStatusService);
  }
}
