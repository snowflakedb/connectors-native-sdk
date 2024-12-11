/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.benchmark.taskreactor;

import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.taskreactor.ComponentNames.TASK_REACTOR_SCHEMA;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.benchmark.BaseBenchmark;
import com.snowflake.connectors.benchmark.EventRow;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.util.sql.SqlTools;
import com.snowflake.connectors.util.sql.TimestampUtil;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskReactorBenchmarkTest extends BaseBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TaskReactorBenchmarkTest.class);
  private static final int WORKERS_NUMBER = 10;
  private static final Duration DEFAULT_SLEEP = Duration.ofSeconds(30);

  WorkItemQueue workItemQueue;

  @BeforeAll
  void setup() {
    workItemQueue = WorkItemQueue.getInstance(session, Identifier.from(TASK_REACTOR_INSTANCE_NAME));

    setupWarehouseReference();
    initializeTaskReactor();
    setWorkersNumber(WORKERS_NUMBER);
  }

  @Test
  void processWorkItemsAndCalculateStatistics() throws InterruptedException {
    int workItemsNumber = 100;
    for (int i = 0; i < workItemsNumber; i++) {
      workItemQueue.push(newWorkItem());
    }

    waitUntilItemsAreProcessedAndEventsArePresentInEventTable(workItemsNumber);

    collectMetricsAndCalculateStatistics();
  }

  private static WorkItem newWorkItem() {
    return new WorkItem(randomId(), randomId(), new Variant("{}"));
  }

  private void collectMetricsAndCalculateStatistics() {
    String query =
        String.format(
            "SELECT\n"
                + "    event.record:name::string AS EVENT_NAME,\n"
                + "    event.record_attributes AS PAYLOAD\n"
                + "FROM PLATFORM_CI_TOOLS.PUBLIC.EVENTS event\n"
                + "    WHERE\n"
                + "        event.resource_attributes:\"snow.database.name\" = '%s'\n"
                + "        AND record_type = 'SPAN_EVENT'"
                + "    ORDER BY event.timestamp DESC",
            application.instanceName.toUpperCase());
    Row[] result = session.sql(query).collect();

    List<EventRow> events = Arrays.stream(result).map(EventRow::from).collect(toList());

    List<Integer> workingTimes =
        events.stream()
            .filter(EventRow::isWorkerWorkingTimeEvent)
            .map(EventRow::getIntValue)
            .collect(toList());
    List<Integer> idleTimes =
        events.stream()
            .filter(EventRow::isWorkerIdleTimeEvent)
            .map(EventRow::getIntValue)
            .collect(toList());

    long workingSum = workingTimes.stream().mapToInt(x -> x).sum();
    long idleSum = idleTimes.stream().mapToInt(x -> x).sum();
    long timeSum = workingSum + idleSum;

    int idleTimeMedian = idleTimes.stream().sorted().collect(toList()).get(idleTimes.size() / 2);
    int workingTimeMedian =
        workingTimes.stream().sorted().collect(toList()).get(workingTimes.size() / 2);
    float workingTimePercent = (float) workingSum / timeSum * 100;
    float idleTimePercent = (float) idleSum / timeSum * 100;
    Instant now = Instant.now();

    logBenchmarkResultRow("PROCESSING_TIME_SECONDS", (float) timeSum / WORKERS_NUMBER / 1000, now);
    logBenchmarkResultRow("WORKER_WORKING_TIME_PERCENT", workingTimePercent, now);
    logBenchmarkResultRow(
        "WORKER_WORKING_TIME_SECONDS_MEDIAN", (float) workingTimeMedian / 1000, now);
    logBenchmarkResultRow("WORKER_IDLE_TIME_PERCENT", idleTimePercent, now);
    logBenchmarkResultRow("WORKER_IDLE_TIME_SECONDS_MEDIAN", (float) idleTimeMedian / 1000, now);
  }

  private void waitUntilItemsAreProcessedAndEventsArePresentInEventTable(int expectedRowNumber) {
    Duration timeout = Duration.ofMinutes(45);
    Instant started = Instant.now();
    while (started.until(Instant.now(), SECONDS) < timeout.get(SECONDS)) {
      String query =
          String.format(
              "SELECT"
                  + "    count(*) "
                  + " FROM PLATFORM_CI_TOOLS.PUBLIC.EVENTS event"
                  + "    WHERE"
                  + "        event.resource_attributes:\"snow.database.name\" = '%s' "
                  + "        AND record_type = 'SPAN_EVENT'"
                  + "        and event.record:name::string = 'TASK_REACTOR_WORKER_WORKING_TIME'"
                  + "    ORDER BY event.timestamp DESC",
              application.instanceName.toUpperCase());
      int count = session.sql(query).first().get().getInt(0);
      LOG.info("Current count = {}", count);
      if (count == expectedRowNumber) {
        return;
      }
      sleep(DEFAULT_SLEEP);
    }
    throw new RuntimeException("Timeout waiting for events in event table.");
  }

  private void logBenchmarkResultRow(String type, float value, Instant timestamp) {
    session
        .sql(
            String.format(
                "INSERT INTO PLATFORM_CI_TOOLS.PUBLIC.BENCHMARK_RESULTS VALUES ('%s', %.2f, '%s')",
                type, value, TimestampUtil.toTimestamp(timestamp)))
        .collect();
  }

  private void setWorkersNumber(int number) {
    session
        .sql(
            String.format(
                "CALL %s.%s(%d, %s)",
                TASK_REACTOR_SCHEMA,
                "SET_WORKERS_NUMBER",
                number,
                asVarchar(TASK_REACTOR_INSTANCE_NAME)))
        .collect();
  }

  private void initializeTaskReactor() {
    SqlTools.callProcedure(
        session,
        "TASK_REACTOR",
        "INITIALIZE_INSTANCE",
        asVarchar(TASK_REACTOR_INSTANCE_NAME),
        asVarchar(WAREHOUSE_REFERENCE.getValue()),
        "null",
        "null",
        "null",
        "null");
  }

  private void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
