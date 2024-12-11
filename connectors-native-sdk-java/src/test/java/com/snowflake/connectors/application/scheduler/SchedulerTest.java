/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.util.snowflake.InMemoryTransactionManager;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SchedulerTest {

  InMemoryIngestionProcessRepository ingestionProcessRepository =
      new InMemoryIngestionProcessRepository();
  OnIngestionScheduledCallback onIngestionScheduledCallback = mock();
  TransactionManager transactionManager = new InMemoryTransactionManager();
  Scheduler scheduler =
      new Scheduler(ingestionProcessRepository, onIngestionScheduledCallback, transactionManager);

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
    assertCallbackWasCalledForProcesses(Collections.singletonList(id));
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
    assertCallbackWasCalledForProcesses(List.of(scheduledProcess1, scheduledProcess2));
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

  @Test
  void shouldRethrowExceptionThrownInACallback() {
    // given
    processWithStatusExists(SCHEDULED);
    processWithStatusExists(SCHEDULED);
    doThrow(new RuntimeException("Some exception"))
        .when(onIngestionScheduledCallback)
        .onIngestionScheduled(any());

    // expect
    assertThatThrownBy(() -> scheduler.runIteration())
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Some exception");
  }

  private String processWithStatusExists(String status) {
    return ingestionProcessRepository.createProcess(
        randomId(), randomId(), "DEFAULT", status, null);
  }

  private void assertProcessHasStatus(String processId, String expectedStatus) {
    IngestionProcess process = ingestionProcessRepository.fetch(processId).get();
    assertThat(process).hasStatus(expectedStatus);
  }

  private void assertCallbackWasCalledForProcesses(List<String> processIds) {
    verify(onIngestionScheduledCallback)
        .onIngestionScheduled(
            assertArg(ids -> assertThat(ids).containsExactlyInAnyOrderElementsOf(processIds)));
  }

  private void assertNoMoreInteractionOnCallback() {
    verifyNoMoreInteractions(onIngestionScheduledCallback);
  }
}
