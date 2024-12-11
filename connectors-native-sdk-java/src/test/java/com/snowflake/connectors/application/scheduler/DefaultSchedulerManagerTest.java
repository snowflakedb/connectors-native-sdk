/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DefaultSchedulerManagerTest {

  private static final ObjectName SCHEDULER_TASK = ObjectName.from("STATE", "SCHEDULER_TASK");
  ConnectorConfigurationService connectorConfigurationService = mock();
  TaskRepository taskRepository = mock();
  TaskLister taskLister = mock();
  TaskRef schedulerTaskRef = mock();
  DefaultSchedulerManager schedulerManager =
      new DefaultSchedulerManager(connectorConfigurationService, taskRepository, taskLister);

  @Test
  void shouldReturnErrorWhenGlobalScheduleIsNotDefined() {
    // given
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant("{}"));
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.empty());

    // expect
    assertThatThrownBy(() -> schedulerManager.createScheduler())
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
    assertThatThrownBy(() -> schedulerManager.createScheduler())
        .isInstanceOf(SchedulerCreationException.class)
        .hasMessage("Global schedule has invalid structure");
  }

  @Test
  void shouldNotThrowExceptionWhenConfigIsValid() {
    // given
    var validConfig =
        Map.of(
            "global_schedule",
            Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *"));
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant(validConfig));
    when(taskRepository.create(any(), anyBoolean(), anyBoolean())).thenReturn(schedulerTaskRef);
    // expect
    assertThatNoException().isThrownBy(() -> schedulerManager.createScheduler());
  }

  @Test
  void shouldThrowExceptionWhenSchedulerIsAlreadyConfigured() {
    // given
    var expected = new Builder(ObjectName.from("state", "task"), "def", "10 MINUTES").build();
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.of(expected));

    // expect
    assertThatThrownBy(() -> schedulerManager.createScheduler())
        .isInstanceOf(SchedulerCreationException.class)
        .hasMessage("Scheduler already created");
  }

  @Test
  void shouldUseWarehouseReferenceWhenWarehouseIdentifierNotFound() {
    // given
    var validConfig =
        Map.of(
            "global_schedule",
            Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *"));
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant(validConfig));
    when(taskRepository.create(any(), anyBoolean(), anyBoolean())).thenReturn(schedulerTaskRef);
    // when
    schedulerManager.createScheduler();
    // then
    verify(taskRepository)
        .create(
            assertArg(
                taskDefinition ->
                    assertThat(taskDefinition.properties().warehouseReference())
                        .isPresent()
                        .hasValueSatisfying(
                            reference ->
                                assertThat(reference.getValue())
                                    .isEqualTo("reference('WAREHOUSE_REFERENCE')"))),
            anyBoolean(),
            anyBoolean());
  }

  @Test
  void shouldUseWarehouseIdentifierWhenIdentifierFound() {
    // given
    var validConfig =
        Map.of(
            "global_schedule",
            Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *"),
            "warehouse",
            "some_warehouse");
    when(connectorConfigurationService.getConfiguration()).thenReturn(new Variant(validConfig));
    when(taskRepository.create(any(), anyBoolean(), anyBoolean())).thenReturn(schedulerTaskRef);
    // when
    schedulerManager.createScheduler();
    // then
    verify(taskRepository)
        .create(
            assertArg(
                taskDefinition ->
                    assertThat(taskDefinition.properties().warehouseIdentifier())
                        .isPresent()
                        .hasValueSatisfying(
                            reference ->
                                assertThat(reference.getValue()).isEqualTo("some_warehouse"))),
            anyBoolean(),
            anyBoolean());
  }

  @Test
  void shouldPauseSchedulerTask() {
    // given
    when(taskRepository.fetch(SCHEDULER_TASK)).thenReturn(schedulerTaskRef);

    // when
    var response = schedulerManager.pauseScheduler();

    // then
    assertThat(response).hasOKResponseCode();
    verify(taskRepository).fetch(SCHEDULER_TASK);
    verify(schedulerTaskRef).suspendIfExists();
  }

  @Test
  void shouldResumeSchedulerTask() {
    // given
    when(taskRepository.fetch(SCHEDULER_TASK)).thenReturn(schedulerTaskRef);

    // when
    var response = schedulerManager.resumeScheduler();

    // then
    assertThat(response).hasOKResponseCode();
    verify(taskRepository).fetch(SCHEDULER_TASK);
    verify(schedulerTaskRef).resumeIfExists();
  }

  @Test
  void shouldReturnFalseIfSchedulerDoesNotExist() {
    // given
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.empty());

    // when
    var response = schedulerManager.schedulerExists();

    // then
    assertThat(response).isFalse();
  }

  @Test
  void shouldReturnTrueIfSchedulerExists() {
    // given
    TaskProperties task = new Builder(SCHEDULER_TASK, "definition", "1 minute").build();
    when(taskLister.showTask(SCHEDULER_TASK)).thenReturn(Optional.of(task));

    // when
    var response = schedulerManager.schedulerExists();

    // then
    assertThat(response).isTrue();
  }

  @Test
  void shouldChangeStartedSchedulerSchedule() {
    // given
    String expectedSchedule = "* * * * *";
    TaskProperties task =
        new Builder(SCHEDULER_TASK, "definition", "1 minute").withState("STARTED").build();
    when(connectorConfigurationService.getConfiguration())
        .thenReturn(
            new Variant(
                Map.of(
                    "global_schedule",
                    Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *"))));
    when(taskRepository.fetch(SCHEDULER_TASK)).thenReturn(schedulerTaskRef);
    when(schedulerTaskRef.fetch()).thenReturn(task);

    // when
    schedulerManager.alterSchedulerSchedule(expectedSchedule);

    // then
    verify(schedulerTaskRef).suspend();
    verify(schedulerTaskRef).resume();
    verify(schedulerTaskRef).alterSchedule(String.format("USING CRON %s UTC", expectedSchedule));
  }

  @Test
  void shouldChangeSuspendedSchedulerSchedule() {
    // given
    String expectedSchedule = "* * * * *";
    TaskProperties task =
        new Builder(SCHEDULER_TASK, "definition", "1 minute").withState("SUSPENDED").build();
    when(connectorConfigurationService.getConfiguration())
        .thenReturn(
            new Variant(
                Map.of(
                    "global_schedule",
                    Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *"))));
    when(taskRepository.fetch(SCHEDULER_TASK)).thenReturn(schedulerTaskRef);
    when(schedulerTaskRef.fetch()).thenReturn(task);

    // when
    schedulerManager.alterSchedulerSchedule(expectedSchedule);

    // then
    verify(schedulerTaskRef).alterSchedule(String.format("USING CRON %s UTC", expectedSchedule));
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
