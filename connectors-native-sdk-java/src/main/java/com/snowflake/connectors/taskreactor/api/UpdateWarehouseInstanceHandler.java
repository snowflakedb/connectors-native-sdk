/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.UpdateTaskReactorTasks;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * Handler for the Task Reactor instance warehouse update. A new instance of the handler must be
 * created using {@link #builder(Session) the builder}.
 */
public class UpdateWarehouseInstanceHandler {

  private static final Logger LOG =
      TaskReactorLogger.getLogger(UpdateWarehouseInstanceHandler.class);

  private final UpdateTaskReactorTasks updateTaskReactorTasks;
  private final ConnectorErrorHelper errorHelper;

  /**
   * Error type for the connector configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "FINALIZE_CONNECTOR_CONFIGURATION_FAILED";

  UpdateWarehouseInstanceHandler(
      UpdateTaskReactorTasks updateTaskReactorTasks, ConnectorErrorHelper errorHelper) {
    this.updateTaskReactorTasks = updateTaskReactorTasks;
    this.errorHelper = errorHelper;
  }

  /**
   * Default handler method for the {@code TASK_REACTOR.UPDATE_WAREHOUSE} procedure.
   *
   * <p>Updates warehouse used by all tasks within the specified Task Reactor instance by inserting
   * the {@code UPDATE_WAREHOUSE} command into the Task Reactor command queue.
   *
   * @param session Snowpark session object
   * @param warehouseName new warehouse name provided to the handler
   * @param instanceSchema task reactor instance name
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #updateWarehouse(String, String) updateWarehouse}
   */
  public static Variant updateWarehouse(
      Session session, String warehouseName, String instanceSchema) {
    var handler = builder(session).build();
    return handler.updateWarehouse(warehouseName, instanceSchema).toVariant();
  }

  /**
   * Returns a new instance of {@link UpdateWarehouseInstanceHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static UpdateWarehouseInstanceHandlerBuilder builder(Session session) {
    return new UpdateWarehouseInstanceHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * @param warehouseName name of the warehouse
   * @param instanceSchema instance name
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse updateWarehouse(String warehouseName, String instanceSchema) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> updateWarehouseBody(warehouseName, instanceSchema));
  }

  private ConnectorResponse updateWarehouseBody(String warehouseName, String instanceSchema) {
    LOG.info(
        "Starting updating warehouse: '{}' for instance: '{}'.", warehouseName, instanceSchema);

    updateTaskReactorTasks.updateInstance(instanceSchema, Identifier.from(warehouseName));
    return ConnectorResponse.success(
        String.format("Warehouse '%s' has been added to the command queue", warehouseName));
  }
}
