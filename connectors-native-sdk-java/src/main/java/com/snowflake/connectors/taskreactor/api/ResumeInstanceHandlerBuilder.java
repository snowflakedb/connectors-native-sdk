/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import com.snowflake.snowpark_java.Session;

public class ResumeInstanceHandlerBuilder {

  private ConnectorErrorHelper errorHelper;
  private final ResumeTaskReactorService resumeTaskReactorService;

  /**
   * Creates a new {@link ResumeInstanceHandlerBuilder}.
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
  public ResumeInstanceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.errorHelper = ConnectorErrorHelper.buildDefault(session, ResumeInstanceHandler.ERROR_TYPE);
    this.resumeTaskReactorService = ResumeTaskReactorService.getInstance(session);
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ResumeInstanceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public ResumeInstanceHandler build() {
    requireNonNull(resumeTaskReactorService);
    requireNonNull(errorHelper);

    return new ResumeInstanceHandler(errorHelper, resumeTaskReactorService);
  }
}
