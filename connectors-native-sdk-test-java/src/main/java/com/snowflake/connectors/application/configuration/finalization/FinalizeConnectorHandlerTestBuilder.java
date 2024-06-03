/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link FinalizeConnectorHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link FinalizeConnectorInputValidator}
 *   <li>{@link SourceValidator}
 *   <li>{@link FinalizeConnectorCallback}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ConnectorStatusService}
 * </ul>
 */
public class FinalizeConnectorHandlerTestBuilder {

  private FinalizeConnectorInputValidator inputValidator;
  private SourceValidator sourceValidator;
  private FinalizeConnectorCallback callback;
  private ConnectorErrorHelper errorHelper;
  private ConnectorStatusService connectorStatusService;
  private FinalizeConnectorSdkCallback sdkCallback;

  /**
   * Creates a new, empty {@link FinalizeConnectorHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public FinalizeConnectorHandlerTestBuilder() {}

  /**
   * Creates a new {@link FinalizeConnectorHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link FinalizeConnectorInputValidator}
   *   <li>a default implementation of {@link SourceValidator}
   *   <li>a default implementation of {@link FinalizeConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ConnectorStatusService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public FinalizeConnectorHandlerTestBuilder(Session session) {
    requireNonNull(session);

    this.inputValidator = new DefaultFinalizeConnectorInputValidator(session);
    this.sourceValidator = new DefaultSourceValidator(session);
    this.callback = new InternalFinalizeConnectorCallback(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, FinalizeConnectorHandler.ERROR_TYPE);
    this.connectorStatusService = ConnectorStatusService.getInstance(session);
  }

  /**
   * Sets the input validator used to build the handler instance.
   *
   * @param inputValidator configuration finalization input validator
   * @return this builder
   */
  public FinalizeConnectorHandlerTestBuilder withInputValidator(
      FinalizeConnectorInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    return this;
  }

  /**
   * Sets the source validator used to build the handler instance.
   *
   * @param sourceValidator source finalization input validator
   * @return this builder
   */
  public FinalizeConnectorHandlerTestBuilder withSourceValidator(SourceValidator sourceValidator) {
    this.sourceValidator = sourceValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback configuration finalization callback
   * @return this builder
   */
  public FinalizeConnectorHandlerTestBuilder withCallback(FinalizeConnectorCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public FinalizeConnectorHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the connector status service used to build the handler instance.
   *
   * @param connectorStatusService connector status service
   * @return this builder
   */
  public FinalizeConnectorHandlerTestBuilder withConnectorStatusService(
      ConnectorStatusService connectorStatusService) {
    this.connectorStatusService = connectorStatusService;
    return this;
  }

  /**
   * Sets the sdk callback used to build the handler instance.
   *
   * @param sdkCallback internal sdk callback
   * @return this builder
   */
  public FinalizeConnectorHandlerTestBuilder withSdkCallback(
      FinalizeConnectorSdkCallback sdkCallback) {
    this.sdkCallback = sdkCallback;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public FinalizeConnectorHandler build() {
    requireNonNull(inputValidator);
    requireNonNull(sourceValidator);
    requireNonNull(callback);
    requireNonNull(errorHelper);
    requireNonNull(connectorStatusService);
    requireNonNull(sdkCallback);

    return new FinalizeConnectorHandler(
        inputValidator,
        sourceValidator,
        callback,
        errorHelper,
        connectorStatusService,
        sdkCallback);
  }
}
