/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RunSchedulerIterationHandlerIntegrationTest extends BaseIntegrationTest {

  private static RunSchedulerIterationHandler handler;
  private static IngestionProcessRepository ingestionProcessRepository;

  @BeforeAll
  static void setup() throws IOException {
    handler = RunSchedulerIterationHandler.builder(session).withCallback(processId -> {}).build();
    ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
  }

  @Test
  void shouldRunSchedulerIteration() {
    // given
    var id = processWithStatusExists("SCHEDULED");

    // when
    handler.runIteration();

    // then
    assertProcessHasStatus(id, "IN_PROGRESS");
  }

  private String processWithStatusExists(String status) {
    return ingestionProcessRepository.createProcess(
        randomId(), randomId(), "DEFAULT", status, null);
  }

  private void assertProcessHasStatus(String processId, String expectedStatus) {
    IngestionProcess process = ingestionProcessRepository.fetch(processId).get();
    assertThat(process).hasStatus(expectedStatus);
  }
}
