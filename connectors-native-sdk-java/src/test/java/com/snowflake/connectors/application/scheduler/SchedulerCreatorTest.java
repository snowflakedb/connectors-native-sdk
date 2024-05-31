/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SchedulerCreatorTest {

  ConnectorConfigurationService connectorConfigurationService = mock();
  TaskRepository taskRepository = mock();
  TaskLister taskLister = mock();
  TaskRef taskRef = mock();
  DefaultSchedulerCreator scheduler =
      new DefaultSchedulerCreator(connectorConfigurationService, taskRepository, taskLister);

  @Test
  void shouldReturnErrorWhenGlobalScheduleIsNotDefined() {
    // given
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant("{}"));
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.empty());

    // expect
    assertThatThrownBy(() -> scheduler.createScheduler())
        .isInstanceOf(SchedulerCreationException.class)
        .hasMessage("Global schedule is not configured");
  }

  @ParameterizedTest
  @MethodSource("invalidScheduleConfigProvider")
  void shouldReturnErrorWhenGlobalScheduleIsInvalid(String scheduleConfig) {
    // given
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant(scheduleConfig));
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.empty());

    // expect
    assertThatThrownBy(() -> scheduler.createScheduler())
        .isInstanceOf(SchedulerCreationException.class)
        .hasMessage("Global schedule has invalid structure");
  }

  @Test
  void shouldNotThrowExceptionWhenConfigIsValid() {
    // given
    String validConfig =
        "{\"global_schedule\": {\"scheduleType\": \"CRON\", \"scheduleDefinition\": \"*/10 * * *"
            + " *\"}}";
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant(validConfig));
    when(taskRepository.create(any(), anyBoolean(), anyBoolean())).thenReturn(taskRef);
    // expect
    assertThatNoException().isThrownBy(() -> scheduler.createScheduler());
  }

  @Test
  void shouldThrowExceptionWhenSchedulerIsAlreadyConfigured() {
    // given
    var expected = new Builder(ObjectName.from("state", "task"), "def", "10 MINUTES").build();
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.of(expected));

    // expect
    assertThatThrownBy(() -> scheduler.createScheduler())
        .isInstanceOf(SchedulerCreationException.class)
        .hasMessage("Scheduler already created");
  }

  private static Stream<Arguments> invalidScheduleConfigProvider() {
    return Stream.of(
        Arguments.of("{\"global_schedule\": {\"scheduleDefinition\": \"*/10 * * * *\"}}"),
        Arguments.of("{\"global_schedule\": {\"scheduleType\": \"CRON\"}}"),
        Arguments.of(
            "{\"global_schedule\": {\"scheduleType\": null, \"scheduleDefinition\": \"*/10 * * *"
                + " *\"}}"));
  }
}
