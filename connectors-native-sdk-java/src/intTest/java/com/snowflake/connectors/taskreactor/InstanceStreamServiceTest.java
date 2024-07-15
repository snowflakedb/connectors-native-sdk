/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.taskreactor.ComponentNames.CONFIG_TABLE;
import static com.snowflake.snowpark_java.Functions.col;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.utils.TaskReactorInstanceConfiguration;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InstanceStreamServiceTest extends BaseTaskReactorIntegrationTest {

  private static final String INSTANCE_NAME = "TEST_INSTANCE";
  private static final ObjectName QUEUE =
      ObjectName.from(INSTANCE_NAME, ComponentNames.QUEUE_TABLE);
  private static final ObjectName COMMANDS_QUEUE =
      ObjectName.from(INSTANCE_NAME, ComponentNames.COMMANDS_QUEUE);
  private static final ObjectName QUEUE_STREAM =
      ObjectName.from(INSTANCE_NAME, ComponentNames.QUEUE_STREAM);
  private static final ObjectName COMMANDS_QUEUE_STREAM =
      ObjectName.from(INSTANCE_NAME, ComponentNames.COMMANDS_QUEUE_STREAM);
  private final InstanceStreamService instanceStreamService =
      InstanceStreamService.getInstance(session);
  private final CommandsQueueRepository commandsQueueRepository =
      CommandsQueueRepository.getInstance(session, Identifier.from(INSTANCE_NAME));
  private static TaskReactorTestInstance instance;

  @BeforeEach
  void setUp() {
    instance =
        TaskReactorTestInstance.buildFromScratch(INSTANCE_NAME, session)
            .withConfig(TaskReactorInstanceConfiguration.builder().build())
            .withQueue()
            .withCommandsQueue()
            .createInstance();
  }

  @AfterEach
  void cleanUp() {
    instance.delete();
  }

  @Test
  void shouldRecreateInstanceStreamsAndUpdateLastStreamsRecreationTime() {
    // given
    var streamsStalenessTimestampBeforeRecreation = getStreamsStalenessTimestamps();
    var currentLastStreamsRecreationTimeValue = getLastStreamsRecreationTime();
    insertDataToQueues();

    // when
    instanceStreamService.recreateStreams(Identifier.from(instance.getName()));

    // then
    streamsStalenessTimestampsAreAfter(streamsStalenessTimestampBeforeRecreation);
    lastStreamsRecreationTimeIsAfter(currentLastStreamsRecreationTimeValue);
    streamsHaveData();

    // when
    deleteDataFromQueues();

    // then
    streamsDoNotHaveData();
  }

  @Test
  void shouldRecreateInstanceStreamsWhenAppropriateAmountOfTimePassedSinceLastRecreation() {
    // given
    setMaxDataExtensionTimeInDaysToSchema(Identifier.from(instance.getName()), 90);
    var streamsStalenessTimestampBeforeRecreation = getStreamsStalenessTimestamps();
    insertDataToQueues();
    instance.updateConfig(
        "LAST_STREAMS_RECREATION",
        Timestamp.from(Instant.now().minus(88, ChronoUnit.DAYS)).toString());

    // when
    instanceStreamService.recreateStreamsIfRequired(Identifier.from(instance.getName()));

    // then
    streamsStalenessTimestampsAreAfter(streamsStalenessTimestampBeforeRecreation);
  }

  @Test
  void
      shouldNotRecreateInstanceStreamsWhenAppropriateAmountOfTimeHasNotPassedSinceLastRecreation() {
    // given
    var streamsStalenessTimestampBeforeRecreation = getStreamsStalenessTimestamps();
    insertDataToQueues();

    // when
    instanceStreamService.recreateStreamsIfRequired(Identifier.from(instance.getName()));

    // then
    streamsStalenessTimestampsAreEqual(streamsStalenessTimestampBeforeRecreation);
  }

  private void lastStreamsRecreationTimeIsAfter(Timestamp currentLastStreamsRecreationTimeValue) {
    assertThat(getLastStreamsRecreationTime()).isAfter(currentLastStreamsRecreationTimeValue);
  }

  private Timestamp getLastStreamsRecreationTime() {
    return Timestamp.valueOf(
        session
            .sql(
                String.format(
                    "SELECT VALUE FROM %s.%s WHERE KEY = 'LAST_STREAMS_RECREATION'",
                    INSTANCE_NAME, CONFIG_TABLE))
            .collect()[0]
            .getString(0));
  }

  private void streamsDoNotHaveData() {
    long commandsQueueRowsNumber =
        session.sql(String.format("SELECT * FROM %s", COMMANDS_QUEUE_STREAM.getValue())).count();
    long queueRowsNumber =
        session.sql(String.format("SELECT * FROM %s", QUEUE_STREAM.getValue())).count();
    assertThat(commandsQueueRowsNumber).isZero();
    assertThat(queueRowsNumber).isZero();
  }

  private void deleteDataFromQueues() {
    session.table(QUEUE.getValue()).delete();
    session.table(COMMANDS_QUEUE.getValue()).delete();
  }

  private void streamsHaveData() {
    long commandsQueueRowsNumber =
        session.sql(String.format("SELECT * FROM %s", COMMANDS_QUEUE_STREAM.getValue())).count();
    long queueRowsNumber =
        session.sql(String.format("SELECT * FROM %s", QUEUE_STREAM.getValue())).count();
    assertThat(commandsQueueRowsNumber).isNotZero();
    assertThat(queueRowsNumber).isNotZero();
  }

  private void setMaxDataExtensionTimeInDaysToSchema(Identifier schemaName, int value) {
    session
        .sql(
            String.format(
                "ALTER SCHEMA %s SET MAX_DATA_EXTENSION_TIME_IN_DAYS = %d",
                schemaName.getValue(), value))
        .collect();
  }

  private void streamsStalenessTimestampsAreAfter(
      Map<String, Timestamp> recentStreamsStalenessTimestamps) {
    var streamsStalenessTimestamps = getStreamsStalenessTimestamps();
    assertThat(streamsStalenessTimestamps.get(ComponentNames.COMMANDS_QUEUE_STREAM))
        .isAfter(recentStreamsStalenessTimestamps.get(ComponentNames.COMMANDS_QUEUE_STREAM));
    assertThat(streamsStalenessTimestamps.get(ComponentNames.QUEUE_STREAM))
        .isAfter(recentStreamsStalenessTimestamps.get(ComponentNames.QUEUE_STREAM));
  }

  private void streamsStalenessTimestampsAreEqual(
      Map<String, Timestamp> recentStreamsStalenessTimestamps) {
    var streamsStalenessTimestamps = getStreamsStalenessTimestamps();
    assertThat(streamsStalenessTimestamps.get(ComponentNames.COMMANDS_QUEUE_STREAM))
        .isEqualTo(recentStreamsStalenessTimestamps.get(ComponentNames.COMMANDS_QUEUE_STREAM));
    assertThat(streamsStalenessTimestamps.get(ComponentNames.QUEUE_STREAM))
        .isEqualTo(recentStreamsStalenessTimestamps.get(ComponentNames.QUEUE_STREAM));
  }

  private void insertDataToQueues() {
    session
        .sql(
            String.format(
                "INSERT INTO %s (RESOURCE_ID, WORKER_PAYLOAD) VALUES ('123', 321)",
                QUEUE.getValue()))
        .collect();
    commandsQueueRepository.add(CommandType.RESUME_INSTANCE, new Variant(""));
  }

  private Map<String, Timestamp> getStreamsStalenessTimestamps() {
    return Arrays.stream(
            session
                .sql(String.format("SHOW STREAMS IN SCHEMA %s", INSTANCE_NAME))
                .select(col("\"name\""), (col("\"stale_after\"")))
                .collect())
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getTimestamp(1)));
  }
}
