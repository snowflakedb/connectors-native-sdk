/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import com.snowflake.connectors.taskreactor.WorkSelectorType;

/** Representation of configuration stored in CONFIG table */
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

  TaskReactorConfig(
      String schema,
      String workerProcedure,
      String workSelectorType,
      String workSelector,
      String expiredWorkSelector,
      String warehouse) {
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
