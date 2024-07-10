/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.application.ingestion.create.CreateResourceHandler;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link CreateResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link SchedulerCreator}
 * </ul>
 */
public class CreateSchedulerHandlerTestBuilder extends CreateSchedulerHandlerBuilder {

  /**
   * Creates a new, empty {@link CreateSchedulerHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public CreateSchedulerHandlerTestBuilder() {}

  /**
   * Creates a new {@link CreateSchedulerHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link SchedulerCreator}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public CreateSchedulerHandlerTestBuilder(Session session) {
    super(session);
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public CreateSchedulerHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    super.withErrorHelper(errorHelper);
    return this;
  }

  /**
   * Sets the scheduler creator used to build the handler instance.
   *
   * @param schedulerCreator scheduler creator
   * @return this builder
   */
  public CreateSchedulerHandlerTestBuilder withSchedulerCreator(SchedulerCreator schedulerCreator) {
    super.schedulerCreator = schedulerCreator;
    return this;
  }
}
