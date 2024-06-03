/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DefaultOnIngestionFinishedCallbackTest {

  private static final Variant METADATA = new Variant(Map.of("prop", "value"));

  InMemoryIngestionProcessRepository ingestionProcessRepository =
      new InMemoryIngestionProcessRepository();
  DefaultOnIngestionFinishedCallback callback =
      new DefaultOnIngestionFinishedCallback(ingestionProcessRepository);

  @ParameterizedTest
  @MethodSource("metadataProvider")
  void shouldUpdateStatusOfIngestionProcessWhenCurrentStatusIsInProgress(Variant metadata) {
    // given
    String processId = processWithStatusExists("IN_PROGRESS");

    // expect
    assertThatNoException().isThrownBy(() -> callback.onIngestionFinished(processId, metadata));
    assertThat(ingestionProcessRepository.fetch(processId).get())
        .hasStatus("SCHEDULED")
        .hasMetadata(metadata);
  }

  private static Stream<Variant> metadataProvider() {
    return Stream.of(null, METADATA);
  }

  @Test
  void shouldNotUpdateStatusOfIngestionProcessWhenCurrentStatusIsCompleted() {
    // given
    String status = "COMPLETED";
    String processId = processWithStatusExists(status);

    // expect
    assertThatNoException().isThrownBy(() -> callback.onIngestionFinished(processId, METADATA));
    assertThat(ingestionProcessRepository.fetch(processId).get())
        .hasStatus(status)
        .hasMetadata(METADATA);
  }

  private String processWithStatusExists(String status) {
    return ingestionProcessRepository.createProcess(
        randomId(), randomId(), "DEFAULT", status, new Variant(Map.of("p", "v")));
  }
}
