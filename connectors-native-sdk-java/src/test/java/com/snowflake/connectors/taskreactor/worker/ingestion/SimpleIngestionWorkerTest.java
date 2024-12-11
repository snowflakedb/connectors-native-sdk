/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import static com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceMetadata;
import static com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.Destination;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.COMPLETED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceId;
import com.snowflake.connectors.application.ingestion.definition.EmptyImplementation;
import com.snowflake.connectors.application.observability.InMemoryIngestionRunRepository;
import com.snowflake.connectors.application.observability.IngestionRun;
import com.snowflake.connectors.common.assertions.NativeSdkAssertions;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.util.snowflake.InMemoryTransactionManager;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SimpleIngestionWorkerTest {
  private final SimpleIngestion<
          CustomResourceId, CustomResourceMetadata, EmptyImplementation, Destination>
      simpleIngestion = mock();
  private final InMemoryIngestionRunRepository ingestionRunRepository =
      new InMemoryIngestionRunRepository();
  private final TransactionManager transactionManager = new InMemoryTransactionManager();
  private final JavaType simpleIngestionWorkItemType =
      TypeFactory.defaultInstance()
          .constructParametricType(
              SimpleIngestionWorkItem.class,
              CustomResourceId.class,
              CustomResourceMetadata.class,
              EmptyImplementation.class,
              Destination.class);
  private final SimpleIngestionWorker<
          CustomResourceId, CustomResourceMetadata, EmptyImplementation, Destination>
      simpleIngestionWorker =
          new SimpleIngestionWorker<>(
              simpleIngestion,
              mock(),
              mock(),
              mock(),
              mock(),
              mock(),
              mock(),
              ingestionRunRepository,
              ingestionRunRepository,
              transactionManager,
              simpleIngestionWorkItemType);

  @BeforeEach
  void beforeEach() {
    ingestionRunRepository.clear();
    reset(simpleIngestion);
  }

  @Test
  void executes_simple_ingestion() {
    // given
    when(simpleIngestion.persistData(any(), any())).thenReturn(99L);

    // when
    simpleIngestionWorker.performWork(workItem());

    // then
    var simpleWorkItem = simpleWorkItem();
    verify(simpleIngestion).fetchData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).persistData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).postIngestion(eq(simpleWorkItem), anyString());

    assertThat(ingestionRunRepository.fetchAllByResourceId("resourceIngestionDefinitionId"))
        .hasSize(1)
        .first()
        .satisfies(
            run ->
                NativeSdkAssertions.assertThat(run)
                    .hasIdAsUUID()
                    .hasIngestionDefinitionId("resourceIngestionDefinitionId")
                    .hasIngestionConfigurationId("ingestionConfigurationId")
                    .hasIngestionProcessId("processId")
                    .hasStartedAt()
                    .hasUpdatedAt()
                    .hasCompletedAt()
                    .hasMetadata(null)
                    .hasStatus(COMPLETED)
                    .hasIngestedRows(99L));

    verify(simpleIngestion).onSuccessfulIngestionCallback(eq(simpleWorkItem), anyString());
  }

  @Test
  void creates_only_one_ingestion_run_if_created() {
    // given
    ingestionRunRepository.startRun(
        "resourceIngestionDefinitionId", "ingestionConfigurationId", "processId", null);

    // when
    simpleIngestionWorker.performWork(workItem());

    // then
    var simpleWorkItem = simpleWorkItem();
    verify(simpleIngestion).fetchData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).persistData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).postIngestion(eq(simpleWorkItem), anyString());

    assertThat(ingestionRunRepository.fetchAllByResourceId("resourceIngestionDefinitionId"))
        .hasSize(1)
        .first()
        .extracting(IngestionRun::getStatus, IngestionRun::getIngestedRows)
        .contains(COMPLETED, 0L);

    verify(simpleIngestion).onSuccessfulIngestionCallback(eq(simpleWorkItem), anyString());
  }

  @Test
  void executes_simple_ingestion_when_ingestion_run_is_already_finished() {
    // given
    String runId =
        ingestionRunRepository.startRun(
            "resourceIngestionDefinitionId", "ingestionConfigurationId", "processId", null);
    ingestionRunRepository.endRun(runId, COMPLETED, 0L, null);

    // when
    simpleIngestionWorker.performWork(workItem());

    // then
    var simpleWorkItem = simpleWorkItem();
    verify(simpleIngestion).fetchData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).persistData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).postIngestion(eq(simpleWorkItem), anyString());

    assertThat(ingestionRunRepository.fetchAllByResourceId("resourceIngestionDefinitionId"))
        .hasSize(2)
        .extracting(IngestionRun::getStatus, IngestionRun::getIngestedRows)
        .contains(tuple(COMPLETED, 0L), tuple(COMPLETED, 0L));

    verify(simpleIngestion).onSuccessfulIngestionCallback(eq(simpleWorkItem), anyString());
  }

  @Test
  void fails_ingestion_run_when_fetch_data_throws_exception() {
    // given
    doThrow(new RuntimeException("SOME ERROR")).when(simpleIngestion).fetchData(any(), any());

    // when
    Throwable throwable = catchThrowable(() -> simpleIngestionWorker.performWork(workItem()));

    // then
    var simpleWorkItem = simpleWorkItem();
    verify(simpleIngestion).fetchData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion, never()).persistData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion, never()).postIngestion(eq(simpleWorkItem), anyString());

    assertThat(throwable).isInstanceOf(RuntimeException.class).hasMessage("SOME ERROR");
    assertThat(ingestionRunRepository.fetchAllByResourceId("resourceIngestionDefinitionId"))
        .hasSize(1)
        .extracting(IngestionRun::getStatus)
        .contains(FAILED);

    verify(simpleIngestion).onFailedIngestionCallback(eq(simpleWorkItem), anyString());
  }

  @Test
  void fails_ingestion_run_when_persist_data_throws_exception() {
    // given
    doThrow(new RuntimeException("SOME ERROR")).when(simpleIngestion).persistData(any(), any());

    // when
    Throwable throwable = catchThrowable(() -> simpleIngestionWorker.performWork(workItem()));

    // then
    var simpleWorkItem = simpleWorkItem();
    verify(simpleIngestion).fetchData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).persistData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion, never()).postIngestion(eq(simpleWorkItem), anyString());

    assertThat(throwable).isInstanceOf(RuntimeException.class).hasMessage("SOME ERROR");
    assertThat(ingestionRunRepository.fetchAllByResourceId("resourceIngestionDefinitionId"))
        .hasSize(1)
        .extracting(IngestionRun::getStatus)
        .contains(FAILED);

    verify(simpleIngestion).onFailedIngestionCallback(eq(simpleWorkItem), anyString());
  }

  @Test
  void does_nothing_when_post_ingestion_throws_exception() {
    // given
    when(simpleIngestion.persistData(any(), any())).thenReturn(99L);
    doThrow(new RuntimeException("SOME ERROR")).when(simpleIngestion).postIngestion(any(), any());

    // when
    Throwable throwable = catchThrowable(() -> simpleIngestionWorker.performWork(workItem()));

    // then
    var simpleWorkItem = simpleWorkItem();
    verify(simpleIngestion).fetchData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).persistData(eq(simpleWorkItem), anyString());
    verify(simpleIngestion).postIngestion(eq(simpleWorkItem), anyString());

    assertThat(throwable).isInstanceOf(RuntimeException.class).hasMessage("SOME ERROR");
    assertThat(ingestionRunRepository.fetchAllByResourceId("resourceIngestionDefinitionId"))
        .hasSize(1)
        .first()
        .extracting(IngestionRun::getStatus, IngestionRun::getIngestedRows)
        .contains(COMPLETED, 99L);

    verify(simpleIngestion).onSuccessfulIngestionCallback(eq(simpleWorkItem), anyString());
  }

  private static WorkItem workItem() {
    return new WorkItem(
        "processId",
        "resourceIngestionDefinitionId",
        new Variant(
            Map.of(
                "id", new Variant("processId"),
                "processId", new Variant("processId"),
                "processType", new Variant("processType"),
                "resourceIngestionDefinitionId", new Variant("resourceIngestionDefinitionId"),
                "ingestionConfigurationId", new Variant("ingestionConfigurationId"),
                "resourceId", new Variant(Map.of("property1", "resourceId", "property2", 2)),
                "ingestionStrategy", new Variant("SOME STRATEGY"),
                "resourceMetadata", new Variant(Map.of("field1", "metadataField", "field2", 333)),
                "destination", new Variant(Map.of("field", "destination")),
                "metadata", new Variant(Map.of("field", Map.of("field2", "value"))))));
  }

  private static SimpleIngestionWorkItem<
          CustomResourceId, CustomResourceMetadata, EmptyImplementation, Destination>
      simpleWorkItem() {
    return new SimpleIngestionWorkItem<>(
        "processId",
        "processId",
        "processType",
        "resourceIngestionDefinitionId",
        "ingestionConfigurationId",
        new CustomResourceId("resourceId", 2),
        "SOME STRATEGY",
        new CustomResourceClasses.CustomResourceMetadata("metadataField", 333),
        null,
        new Destination("destination"),
        new Variant(Map.of("field", Map.of("field2", "value"))));
  }
}
