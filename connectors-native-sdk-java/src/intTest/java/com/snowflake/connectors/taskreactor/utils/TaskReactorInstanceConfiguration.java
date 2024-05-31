/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import static com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest.EXPIRED_WORK_SELECTOR_LOCATION;
import static com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest.WORKER_PROCEDURE_LOCATION;
import static com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest.WORK_SELECTOR_LOCATION;
import static com.snowflake.connectors.taskreactor.utils.Constants.WorkSelectorType.PROCEDURE;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.taskreactor.utils.Constants.WorkSelectorType;

public class TaskReactorInstanceConfiguration {
  private final String workerProcedureName;
  private final WorkSelectorType workSelectorType;
  private final String workSelectorName;
  private final String expiredWorkSelector;

  public TaskReactorInstanceConfiguration(
      String workerProcedureName,
      WorkSelectorType workSelectorType,
      String workSelectorName,
      String expiredWorkSelector) {
    this.workerProcedureName = workerProcedureName;
    this.workSelectorType = workSelectorType;
    this.workSelectorName = workSelectorName;
    this.expiredWorkSelector = expiredWorkSelector;
  }

  public static InstanceConfigurationBuilder builder() {
    return new InstanceConfigurationBuilder();
  }

  public String getWorkerProcedureName() {
    return workerProcedureName;
  }

  public WorkSelectorType getWorkSelectorType() {
    return workSelectorType;
  }

  public String getWorkSelectorName() {
    return workSelectorName;
  }

  public String getExpiredWorkSelector() {
    return expiredWorkSelector;
  }

  public static class InstanceConfigurationBuilder {
    private String workerProcedureName = WORKER_PROCEDURE_LOCATION;
    private WorkSelectorType workSelectorType = PROCEDURE;
    private String workSelectorName = WORK_SELECTOR_LOCATION;
    private String expiredWorkSelectorName = EXPIRED_WORK_SELECTOR_LOCATION;

    public TaskReactorInstanceConfiguration build() {
      return new TaskReactorInstanceConfiguration(
          requireNonNull(this.workerProcedureName),
          requireNonNull(this.workSelectorType),
          requireNonNull(this.workSelectorName),
          requireNonNull(this.expiredWorkSelectorName));
    }

    public InstanceConfigurationBuilder withCustomWorkerProcedure(String workerProcedureName) {
      this.workerProcedureName = workerProcedureName;
      return this;
    }

    public InstanceConfigurationBuilder withCustomWorkSelectorType(WorkSelectorType type) {
      this.workSelectorType = type;
      return this;
    }

    public InstanceConfigurationBuilder withCustomWorkSelector(String workSelectorName) {
      this.workSelectorName = workSelectorName;
      return this;
    }

    public InstanceConfigurationBuilder setExpiredWorkSelectorName(String expiredWorkSelectorName) {
      this.expiredWorkSelectorName = expiredWorkSelectorName;
      return this;
    }
  }
}
