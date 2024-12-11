/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.telemetry;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.util.sql.TimestampUtil;
import com.snowflake.telemetry.Telemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/** Telemetry utility for Task Reactor. */
public class TaskReactorTelemetry {

  private static final String WORKER_IDLE_TIME_EVENT = "TASK_REACTOR_WORKER_IDLE_TIME";
  private static final String WORKER_WORKING_TIME_EVENT = "TASK_REACTOR_WORKER_WORKING_TIME";
  private static final String WORK_ITEM_WAITING_TIME_EVENT =
      "TASK_REACTOR_WORK_ITEM_WAITING_IN_QUEUE_TIME";
  private static final String WORK_ITEMS_NUMBER_IN_QUEUE_EVENT =
      "TASK_REACTOR_WORK_ITEMS_NUMBER_IN_QUEUE";
  private static final String WORKER_STATUS_EVENT = "TASK_REACTOR_WORKER_STATUS";

  private static final String TASK_REACTOR_INSTANCE_ATTRIBUTE = "task_reactor_instance";
  private static final String WORKER_ID_ATTRIBUTE = "worker_id";

  private static final String VALUE_ATTRIBUTE = "value";

  /**
   * Sets the value of the Task Reactor instance name span attribute.
   *
   * @param value attribute value
   */
  public static void setTaskReactorInstanceNameSpanAttribute(Identifier value) {
    Telemetry.setSpanAttribute(TASK_REACTOR_INSTANCE_ATTRIBUTE, value.getValue());
  }

  /**
   * Sets the value of the worker id span attribute.
   *
   * @param workerId attribute value
   */
  public static void setWorkerIdSpanAttribute(WorkerId workerId) {
    Telemetry.setSpanAttribute(WORKER_ID_ATTRIBUTE, workerId.value());
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
   * Adds a new event for the time of work item waiting in a dispatcher queue.
   *
   * @param startTime time when a work item was inserted to a dispatcher queue
   * @param endTime time when a work item was removed from a dispatcher queue and inserted to worker
   *     queue
   */
  public static void addWorkItemWaitingInQueueTimeEvent(Timestamp startTime, Instant endTime) {
    Instant start = TimestampUtil.toInstant(startTime);
    addEvent(WORK_ITEM_WAITING_TIME_EVENT, Duration.between(start, endTime).toMillis());
  }

  /**
   * Adds a new event for the number of work items present in a dispatcher queue.
   *
   * @param number number of work items
   * @param instanceName Task Reactor instance name
   */
  public static void addWorkItemsNumberInQueueEvent(long number, Identifier instanceName) {
    Attributes attributes =
        Attributes.builder()
            .put(VALUE_ATTRIBUTE, number)
            .put(TASK_REACTOR_INSTANCE_ATTRIBUTE, instanceName.getValue())
            .build();
    addEvent(WORK_ITEMS_NUMBER_IN_QUEUE_EVENT, attributes);
  }

  /**
   * Adds a new event which contains the number of workers in each worker status. The event contains
   * also an additional field which presents the total number of workers.
   *
   * @param statuses number of workers for each status which should be logged
   */
  public static void addWorkerStatusEvent(Map<String, Integer> statuses) {
    AttributesBuilder attributes = Attributes.builder();
    statuses.forEach(attributes::put);
    int workersNumber = statuses.values().stream().mapToInt(Integer::intValue).sum();
    attributes.put("TOTAL", workersNumber);
    addEvent(WORKER_STATUS_EVENT, attributes.build());
  }

  /**
   * Adds a new telemetry event.
   *
   * @param eventName event name
   * @param value event value
   */
  private static void addEvent(String eventName, long value) {
    Attributes attributes = Attributes.builder().put(VALUE_ATTRIBUTE, value).build();
    addEvent(eventName, attributes);
  }

  private static void addEvent(String eventName, Attributes attributes) {
    Telemetry.addEvent(eventName, attributes);
  }
}
