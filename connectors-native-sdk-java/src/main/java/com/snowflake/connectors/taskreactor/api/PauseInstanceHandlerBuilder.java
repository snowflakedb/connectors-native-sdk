/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link PauseInstanceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class PauseInstanceHandlerBuilder {

  private ConnectorErrorHelper errorHelper;
  private final PauseTaskReactorService pauseTaskReactorService;

  /**
   * Creates a new {@link PauseInstanceHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public PauseInstanceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, PauseInstanceHandler.ERROR_TYPE);
    this.pauseTaskReactorService = PauseTaskReactorService.getInstance(session);
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public PauseInstanceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public PauseInstanceHandler build() {
    requireNonNull(errorHelper);
    requireNonNull(pauseTaskReactorService);

    return new PauseInstanceHandler(errorHelper, pauseTaskReactorService);
  }
}
