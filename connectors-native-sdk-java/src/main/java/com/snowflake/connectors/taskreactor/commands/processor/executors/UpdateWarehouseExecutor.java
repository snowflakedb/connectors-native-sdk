/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.config.ConfigRepository;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerTaskManager;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.snowpark_java.Session;
import java.util.Optional;

/** Executor of the {@code UPDATE_WAREHOUSE} command. */
public class UpdateWarehouseExecutor implements CommandExecutor {

  private final ConfigRepository configRepository;
  private final WorkerRegistryService workerRegistry;
  private final WorkerTaskManager workerTaskManager;

  private UpdateWarehouseExecutor(
      ConfigRepository configRepository,
      WorkerRegistryService workerRegistry,
      WorkerTaskManager workerTaskManager) {
    this.configRepository = configRepository;
    this.workerRegistry = workerRegistry;
    this.workerTaskManager = workerTaskManager;
  }

  @Override
  public void execute(Command command) {
    var warehouseName = extractWarehouse(command);

    configRepository.update("WAREHOUSE", warehouseName);
    var workerIds = workerRegistry.getActiveProvisioningOrUpForDeletionWorkers();

    for (WorkerId workerId : workerIds) {
      workerTaskManager.alterWarehouse(workerId, warehouseName);
    }
  }

  private String extractWarehouse(Command command) {
    return Optional.ofNullable(command.getPayload().asMap().get("warehouse_name"))
        .orElseThrow(() -> new InvalidCommandException(command))
        .asString();
  }

  /**
   * Creates new instance of {@link UpdateWarehouseExecutor}
   *
   * @param session Snowpark session object
   * @param instanceName task reactor schema name
   * @return new instance of {@link UpdateWarehouseExecutor}
   */
  public static UpdateWarehouseExecutor getInstance(Session session, Identifier instanceName) {
    return new UpdateWarehouseExecutor(
        ConfigRepository.getInstance(session, instanceName),
        WorkerRegistryService.from(session, instanceName),
        WorkerTaskManager.from(session, instanceName));
  }
}
