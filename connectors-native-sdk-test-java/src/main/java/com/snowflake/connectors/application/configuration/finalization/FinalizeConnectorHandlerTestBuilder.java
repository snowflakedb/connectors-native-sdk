/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

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
public class FinalizeConnectorHandlerTestBuilder extends FinalizeConnectorHandlerBuilder {

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
    super(session);
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
    this.finalizeConnectorSdkCallback = sdkCallback;
    return this;
  }
}
