/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.ingestion.process.CrudIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link RunSchedulerIterationHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link OnIngestionScheduledCallback}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class RunSchedulerIterationHandlerBuilder {

  private Scheduler scheduler;
  private ConnectorErrorHelper errorHelper;
  private final CrudIngestionProcessRepository ingestionProcessRepository;
  private final TransactionManager transactionManager;

  /**
   * Creates a new {@link RunSchedulerIterationHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link OnIngestionScheduledCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public RunSchedulerIterationHandlerBuilder(Session session) {
    requireNonNull(session);

    this.ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    this.transactionManager = TransactionManager.getInstance(session);
    this.scheduler =
        new Scheduler(
            ingestionProcessRepository,
            OnIngestionScheduledCallback.getInstance(session),
            transactionManager);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, RunSchedulerIterationHandler.ERROR_TYPE);
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback on ingestion scheduled callback
   * @return this builder
   */
  public RunSchedulerIterationHandlerBuilder withCallback(OnIngestionScheduledCallback callback) {
    this.scheduler = new Scheduler(ingestionProcessRepository, callback, transactionManager);
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public RunSchedulerIterationHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public RunSchedulerIterationHandler build() {
    requireNonNull(scheduler);
    requireNonNull(errorHelper);

    return new RunSchedulerIterationHandler(scheduler, errorHelper);
  }
}
