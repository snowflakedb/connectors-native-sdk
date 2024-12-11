/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.scheduler.CreateSchedulerHandler.ERROR_TYPE;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link CreateSchedulerHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class CreateSchedulerHandlerTestBuilder extends CreateSchedulerHandlerBuilder {

  /**
   * Creates a new {@link CreateSchedulerHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * <ul>
   *   <li>{@link SchedulerManager} built using {@link SchedulerManager#getInstance(Session)
   *       getInstance}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public CreateSchedulerHandlerTestBuilder(Session session) {
    requireNonNull(session);

    super.schedulerManager = SchedulerManager.getInstance(session);
    this.errorHelper = ConnectorErrorHelper.buildDefault(session, ERROR_TYPE);
  }

  /** Constructor used by the test builder implementation. */
  public CreateSchedulerHandlerTestBuilder() {}

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public CreateSchedulerHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    super.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the scheduler manager used to build the handler instance.
   *
   * @param schedulerManager scheduler manager
   * @return this builder
   */
  public CreateSchedulerHandlerTestBuilder withSchedulerManager(SchedulerManager schedulerManager) {
    this.schedulerManager = schedulerManager;
    return this;
  }
}
