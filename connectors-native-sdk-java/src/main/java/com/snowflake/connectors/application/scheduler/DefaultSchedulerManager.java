/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.GLOBAL_SCHEDULE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.WAREHOUSE;

import com.snowflake.connectors.application.configuration.DefaultConfigurationMap;
import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.ingestion.definition.ScheduleType;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.Reference;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import java.util.Optional;

/** Default implementation of {@link SchedulerManager}. */
class DefaultSchedulerManager implements SchedulerManager {

  private final ConnectorConfigurationService connectorConfigurationService;
  private final TaskRepository taskRepository;
  private final TaskLister taskLister;

  DefaultSchedulerManager(
      ConnectorConfigurationService connectorConfigurationService,
      TaskRepository taskRepository,
      TaskLister taskLister) {
    this.connectorConfigurationService = connectorConfigurationService;
    this.taskRepository = taskRepository;
    this.taskLister = taskLister;
  }

  public ConnectorResponse createScheduler() {
    validateIfSchedulerAlreadyExists();
    DefaultConfigurationMap configuration =
        new DefaultConfigurationMap(connectorConfigurationService.getConfiguration().asMap());
    GlobalSchedule globalSchedule = fetchGlobalSchedule(configuration);
    TaskProperties taskProperties = createSchedulerTaskObject(globalSchedule, configuration);
    TaskDefinition taskDefinition = new TaskDefinition(taskProperties);
    TaskRef schedulerTaskRef = taskRepository.create(taskDefinition, true, false);
    schedulerTaskRef.grantMonitorPrivilegeToRole("ADMIN");
    schedulerTaskRef.resume();
    return ConnectorResponse.success();
  }

  public ConnectorResponse resumeScheduler() {
    var schedulerTask = taskRepository.fetch(Scheduler.SCHEDULER_TASK);
    schedulerTask.resumeIfExists();
    return ConnectorResponse.success();
  }

  public ConnectorResponse pauseScheduler() {
    var schedulerTask = taskRepository.fetch(Scheduler.SCHEDULER_TASK);
    schedulerTask.suspendIfExists();
    return ConnectorResponse.success();
  }

  public boolean schedulerExists() {
    return taskLister.showTask(Scheduler.SCHEDULER_TASK).isPresent();
  }

  @Override
  public void alterSchedulerSchedule(String schedule) {
    DefaultConfigurationMap configuration =
        new DefaultConfigurationMap(this.connectorConfigurationService.getConfiguration().asMap());
    GlobalSchedule globalSchedule = fetchGlobalSchedule(configuration);

    if (globalSchedule.scheduleDefinition.equals(schedule)) {
      return;
    }

    TaskRef schedulerTask = this.taskRepository.fetch(Scheduler.SCHEDULER_TASK);
    boolean resumed = schedulerTask.fetch().state().equalsIgnoreCase("STARTED");

    if (resumed) {
      schedulerTask.suspend();
    }

    schedulerTask.alterSchedule(String.format("USING CRON %s UTC", schedule));

    if (resumed) {
      schedulerTask.resume();
    }
  }

  private void validateIfSchedulerAlreadyExists() {
    if (schedulerExists()) {
      throw new SchedulerCreationException("Scheduler already created");
    }
  }

  private GlobalSchedule fetchGlobalSchedule(DefaultConfigurationMap configuration) {
    GlobalSchedule globalSchedule =
        configuration
            .get(GLOBAL_SCHEDULE.getPropertyName(), GlobalSchedule.class)
            .orElseThrow(() -> new SchedulerCreationException("Global schedule is not configured"));
    if (globalSchedule.scheduleType == null || globalSchedule.scheduleDefinition == null) {
      throw new SchedulerCreationException("Global schedule has invalid structure");
    }
    return globalSchedule;
  }

  private TaskProperties createSchedulerTaskObject(
      GlobalSchedule globalSchedule, DefaultConfigurationMap configuration) {
    String definition = "CALL PUBLIC.RUN_SCHEDULER_ITERATION()";
    String schedule = toTaskSchedule(globalSchedule);
    Optional<Identifier> warehouseIdentifier =
        configuration.get(WAREHOUSE.getPropertyName(), String.class).map(Identifier::from);

    TaskProperties.Builder builder =
        new TaskProperties.Builder(Scheduler.SCHEDULER_TASK, definition, schedule);
    warehouseIdentifier.ifPresentOrElse(
        builder::withWarehouse, () -> builder.withWarehouse(Reference.from("WAREHOUSE_REFERENCE")));
    return builder.withAllowOverlappingExecution(false).withSuspendTaskAfterNumFailures(0).build();
  }

  private String toTaskSchedule(GlobalSchedule globalSchedule) {
    if (globalSchedule.scheduleType == ScheduleType.CRON) {
      return "USING CRON " + globalSchedule.scheduleDefinition + " UTC";
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported.", globalSchedule.scheduleDefinition));
    }
  }

  private static class GlobalSchedule {
    public ScheduleType scheduleType;
    public String scheduleDefinition;

    private GlobalSchedule() {}
  }
}
