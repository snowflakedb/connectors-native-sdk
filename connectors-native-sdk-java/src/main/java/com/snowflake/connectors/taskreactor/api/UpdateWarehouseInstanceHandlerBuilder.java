/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.task.UpdateTaskReactorTasks;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link UpdateWarehouseInstanceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link UpdateTaskReactorTasks}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class UpdateWarehouseInstanceHandlerBuilder {

  private UpdateTaskReactorTasks updateTaskReactorTasks;
  private ConnectorErrorHelper errorHelper;

  /**
   * Creates a new {@link UpdateWarehouseInstanceHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link UpdateTaskReactorTasks}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  UpdateWarehouseInstanceHandlerBuilder(Session session) {
    requireNonNull(session);

    this.updateTaskReactorTasks = UpdateTaskReactorTasks.getInstance(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, UpdateWarehouseInstanceHandler.ERROR_TYPE);
  }

  /**
   * Sets the input validator used to build the handler instance.
   *
   * @param updateTaskReactorTasks updates operations on tasks
   * @return this builder
   */
  public UpdateWarehouseInstanceHandlerBuilder withUpdateTaskReactorTasks(
      UpdateTaskReactorTasks updateTaskReactorTasks) {
    this.updateTaskReactorTasks = updateTaskReactorTasks;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public UpdateWarehouseInstanceHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public UpdateWarehouseInstanceHandler build() {
    requireNonNull(updateTaskReactorTasks);
    requireNonNull(errorHelper);

    return new UpdateWarehouseInstanceHandler(updateTaskReactorTasks, errorHelper);
  }
}
