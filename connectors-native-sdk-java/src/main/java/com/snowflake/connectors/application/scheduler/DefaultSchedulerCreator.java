/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.GLOBAL_SCHEDULE;
import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;
import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.ingestion.definition.ScheduleType;
import com.snowflake.connectors.common.object.Reference;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link SchedulerCreator}. */
class DefaultSchedulerCreator implements SchedulerCreator {

  private final ConnectorConfigurationService connectorConfigurationService;
  private final TaskRepository taskRepository;
  private final TaskLister taskLister;

  DefaultSchedulerCreator(
      ConnectorConfigurationService connectorConfigurationService,
      TaskRepository taskRepository,
      TaskLister taskLister) {
    this.connectorConfigurationService = connectorConfigurationService;
    this.taskRepository = taskRepository;
    this.taskLister = taskLister;
  }

  @Override
  public ConnectorResponse createScheduler() {
    validateIfSchedulerAlreadyExists();
    GlobalSchedule globalSchedule = fetchGlobalSchedule();
    TaskProperties taskProperties = createSchedulerTaskObject(globalSchedule);
    TaskDefinition taskDefinition = new TaskDefinition(taskProperties);
    TaskRef schedulerTaskRef = taskRepository.create(taskDefinition, true, false);

    schedulerTaskRef.grantMonitorPrivilegeToRole("ADMIN");
    schedulerTaskRef.resume();
    return ConnectorResponse.success();
  }

  private void validateIfSchedulerAlreadyExists() {
    var task = taskLister.showTask(SCHEDULER_TASK);
    if (task.isPresent()) {
      throw new SchedulerCreationException("Scheduler already created");
    }
  }

  private GlobalSchedule fetchGlobalSchedule() {
    Variant configuration = connectorConfigurationService.getConfiguration();
    Variant globalScheduleVariant = configuration.asMap().get(GLOBAL_SCHEDULE.getPropertyName());
    if (globalScheduleVariant == null) {
      throw new SchedulerCreationException("Global schedule is not configured");
    }

    GlobalSchedule globalSchedule = mapVariant(globalScheduleVariant, GlobalSchedule.class);
    if (globalSchedule.scheduleType == null || globalSchedule.scheduleDefinition == null) {
      throw new SchedulerCreationException("Global schedule has invalid structure");
    }
    return globalSchedule;
  }

  private TaskProperties createSchedulerTaskObject(GlobalSchedule globalSchedule) {
    String definition = "CALL PUBLIC.RUN_SCHEDULER_ITERATION()";
    String schedule = toTaskSchedule(globalSchedule);

    return new TaskProperties.Builder(SCHEDULER_TASK, definition, schedule)
        .withWarehouse(Reference.from("WAREHOUSE_REFERENCE"))
        .withAllowOverlappingExecution(false)
        .build();
  }

  private String toTaskSchedule(GlobalSchedule globalSchedule) {
    if (globalSchedule.scheduleType == ScheduleType.CRON) {
      return "USING CRON " + globalSchedule.scheduleDefinition + " UTC";
    } else {
      // todo interpreter for normal schedule
      throw new UnsupportedOperationException();
    }
  }

  private static class GlobalSchedule {
    public ScheduleType scheduleType;
    public String scheduleDefinition;
  }
}
