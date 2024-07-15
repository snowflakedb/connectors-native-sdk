/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SchedulerTest {

  InMemoryIngestionProcessRepository ingestionProcessRepository =
      new InMemoryIngestionProcessRepository();
  OnIngestionScheduledCallback onIngestionScheduledCallback = mock();
  Scheduler scheduler = new Scheduler(ingestionProcessRepository, onIngestionScheduledCallback);

  @AfterEach
  void cleanUp() {
    ingestionProcessRepository.clear();
    reset(onIngestionScheduledCallback);
  }

  @Test
  void shouldProcessAnIngestionProcess() {
    // given
    var id = processWithStatusExists(SCHEDULED);

    // when
    scheduler.runIteration();

    // then
    assertProcessHasStatus(id, IN_PROGRESS);
    assertCallbackWasCalledForProcess(id);
  }

  @Test
  void shouldProcessOnlyIngestionProcessesWithScheduledStatus() {
    // given
    var scheduledProcess1 = processWithStatusExists(SCHEDULED);
    var scheduledProcess2 = processWithStatusExists(SCHEDULED);
    var inProgressProcess1 = processWithStatusExists(IN_PROGRESS);
    var inProgressProcess2 = processWithStatusExists(IN_PROGRESS);
    var finishedProcess1 = processWithStatusExists(FINISHED);
    var finishedProcess2 = processWithStatusExists(FINISHED);

    // when
    scheduler.runIteration();

    // then
    assertProcessHasStatus(scheduledProcess1, IN_PROGRESS);
    assertProcessHasStatus(scheduledProcess2, IN_PROGRESS);
    assertProcessHasStatus(inProgressProcess1, IN_PROGRESS);
    assertProcessHasStatus(inProgressProcess2, IN_PROGRESS);
    assertProcessHasStatus(finishedProcess1, FINISHED);
    assertProcessHasStatus(finishedProcess2, FINISHED);
    assertCallbackWasCalledForProcess(scheduledProcess1);
    assertCallbackWasCalledForProcess(scheduledProcess2);
    assertNoMoreInteractionOnCallback();
  }

  @Test
  void shouldDoNothingWhenNoProcessExists() {
    // when
    scheduler.runIteration();

    // then
    assertNoMoreInteractionOnCallback();
  }

  @Test
  void shouldDoNothingWhenNoProcessWithScheduledStatusExists() {
    // given
    var inProgressProcess = processWithStatusExists(IN_PROGRESS);
    var finishedProcess = processWithStatusExists(FINISHED);

    // when
    scheduler.runIteration();

    // then
    assertProcessHasStatus(inProgressProcess, IN_PROGRESS);
    assertProcessHasStatus(finishedProcess, FINISHED);
    assertNoMoreInteractionOnCallback();
  }

  private String processWithStatusExists(String status) {
    return ingestionProcessRepository.createProcess(
        randomId(), randomId(), "DEFAULT", status, null);
  }

  private void assertProcessHasStatus(String processId, String expectedStatus) {
    IngestionProcess process = ingestionProcessRepository.fetch(processId).get();
    assertThat(process).hasStatus(expectedStatus);
  }

  private void assertCallbackWasCalledForProcess(String processId) {
    verify(onIngestionScheduledCallback).onIngestionScheduled(processId);
  }

  private void assertNoMoreInteractionOnCallback() {
    verifyNoMoreInteractions(onIngestionScheduledCallback);
  }
}
