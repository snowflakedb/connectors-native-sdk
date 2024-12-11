/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link UpdateConnectionConfigurationHandler}.
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
 * </ul>
 */
public class UpdateConnectionConfigurationHandlerBuilder {

  private ConnectionConfigurationInputValidator inputValidator;
  private ConnectionConfigurationCallback draftCallback;
  private DraftConnectionValidator draftConnectionValidator;
  private ConnectionConfigurationCallback callback;
  private ConnectionValidator connectionValidator;
  private ConnectorErrorHelper errorHelper;
  private final ConnectionConfigurationService connectionConfigurationService;
  private final ConnectorStatusService connectorStatusService;

  /**
   * Creates a new {@link UpdateConnectionConfigurationHandlerBuilder}.
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
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  UpdateConnectionConfigurationHandlerBuilder(Session session) {
    requireNonNull(session);

    this.inputValidator = new DefaultUpdateConnectionConfigurationInputValidator(session);
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
   * Sets the input validator used to build the handler instance.
   *
   * @param inputValidator connection configuration input validator
   * @return this builder
   */
  public UpdateConnectionConfigurationHandlerBuilder withInputValidator(
      ConnectionConfigurationInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    return this;
  }

  /**
   * Sets the draft callback used to build the handler instance.
   *
   * @param draftCallback connector configuration callback
   * @return this builder
   */
  public UpdateConnectionConfigurationHandlerBuilder withDraftCallback(
      ConnectionConfigurationCallback draftCallback) {
    this.draftCallback = draftCallback;
    return this;
  }

  /**
   * Sets the draft connection validator used to build the handler instance.
   *
   * @param draftConnectionValidator draft connection validator
   * @return this builder
   */
  public UpdateConnectionConfigurationHandlerBuilder withDraftConnectionValidator(
      DraftConnectionValidator draftConnectionValidator) {
    this.draftConnectionValidator = draftConnectionValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback connector configuration callback
   * @return this builder
   */
  public UpdateConnectionConfigurationHandlerBuilder withCallback(
      ConnectionConfigurationCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the connection validator used to build the handler instance.
   *
   * @param connectionValidator connection validator
   * @return this builder
   */
  public UpdateConnectionConfigurationHandlerBuilder withConnectionValidator(
      ConnectionValidator connectionValidator) {
    this.connectionValidator = connectionValidator;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public UpdateConnectionConfigurationHandlerBuilder withErrorHelper(
      ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
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
