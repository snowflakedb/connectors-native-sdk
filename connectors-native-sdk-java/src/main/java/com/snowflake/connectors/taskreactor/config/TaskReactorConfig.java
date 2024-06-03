/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.snowflake.connectors.taskreactor.WorkSelectorType;

/** Representation of configuration stored in CONFIG table */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskReactorConfig {

  /** Task reactor instance schema. */
  private final String schema;

  /** Worker procedure name. */
  private final String workerProcedure;

  /** Work selector type (procedure or view). */
  private final WorkSelectorType workSelectorType;

  /** Work selector object name. */
  private final String workSelector;

  /** Expired work selector object name. */
  private final String expiredWorkSelector;

  /** Warehouse name. */
  private final String warehouse;

  @JsonCreator
  TaskReactorConfig(
      @JsonProperty("SCHEMA") String schema,
      @JsonProperty("WORKER_PROCEDURE") String workerProcedure,
      @JsonProperty("WORK_SELECTOR_TYPE") String workSelectorType,
      @JsonProperty("WORK_SELECTOR") String workSelector,
      @JsonProperty("EXPIRED_WORK_SELECTOR") String expiredWorkSelector,
      @JsonProperty("WAREHOUSE") String warehouse) {
    this.schema = schema;
    this.workerProcedure = workerProcedure;
    this.workSelectorType = WorkSelectorType.valueOf(workSelectorType);
    this.workSelector = workSelector;
    this.expiredWorkSelector = expiredWorkSelector;
    this.warehouse = warehouse;
  }

  /**
   * Returns the Task Reactor instance schema.
   *
   * @return Task Reactor instance schema
   */
  public String schema() {
    return schema;
  }

  /**
   * Returns the worker procedure name.
   *
   * @return worker procedure name
   */
  public String workerProcedure() {
    return workerProcedure;
  }

  /**
   * Returns the work selector type.
   *
   * @return work selector type
   */
  public WorkSelectorType workSelectorType() {
    return workSelectorType;
  }

  /**
   * Returns the warehouse name.
   *
   * @return warehouse name
   */
  public String warehouse() {
    return warehouse;
  }

  /**
   * Returns the work selector object name.
   *
   * @return work selector object name
   */
  public String workSelector() {
    return workSelector;
  }

  /**
   * Returns the expired work selector object name.
   *
   * @return expired work selector object name
   */
  public String expiredWorkSelector() {
    return expiredWorkSelector;
  }
}
