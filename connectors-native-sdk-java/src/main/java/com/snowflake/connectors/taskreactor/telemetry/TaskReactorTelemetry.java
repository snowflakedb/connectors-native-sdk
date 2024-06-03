/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.telemetry;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.telemetry.Telemetry;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.time.Instant;

/** Telemetry utility for Task Reactor. */
public class TaskReactorTelemetry {

  private static final String WORKER_IDLE_TIME_EVENT = "TASK_REACTOR_WORKER_IDLE_TIME";
  private static final String WORKER_WORKING_TIME_EVENT = "TASK_REACTOR_WORKER_WORKING_TIME";

  private static final String TASK_REACTOR_INSTANCE_SPAN_ATTRIBUTE = "task_reactor_instance";
  private static final String WORKER_ID_SPAN_ATTRIBUTE = "worker_id";

  private static final String VALUE_ATTRIBUTE = "value";

  /**
   * Sets the value of the Task Reactor instance name span attribute.
   *
   * @param value attribute value
   */
  public static void setTaskReactorInstanceNameSpanAttribute(Identifier value) {
    Telemetry.setSpanAttribute(TASK_REACTOR_INSTANCE_SPAN_ATTRIBUTE, value.getName());
  }

  /**
   * Sets the value of the worker id span attribute.
   *
   * @param workerId attribute value
   */
  public static void setWorkerIdSpanAttribute(WorkerId workerId) {
    Telemetry.setSpanAttribute(WORKER_ID_SPAN_ATTRIBUTE, workerId.value());
  }

  /**
   * Adds a new event for the worker idle time.
   *
   * @param startTime idle start time
   * @param endTime idle end time
   */
  public static void addWorkerIdleTimeEvent(Instant startTime, Instant endTime) {
    addEvent(WORKER_IDLE_TIME_EVENT, Duration.between(startTime, endTime).toMillis());
  }

  /**
   * Adds a new event for the worker working time.
   *
   * @param startTime working start time
   * @param endTime working end time
   */
  public static void addWorkerWorkingTimeEvent(Instant startTime, Instant endTime) {
    addEvent(WORKER_WORKING_TIME_EVENT, Duration.between(startTime, endTime).toMillis());
  }

  /**
   * Adds a new telemetry event.
   *
   * @param eventName event name
   * @param value event value
   */
  public static void addEvent(String eventName, long value) {
    Attributes attributes = Attributes.builder().put(VALUE_ATTRIBUTE, value).build();
    Telemetry.addEvent(eventName, attributes);
  }
}
