package com.snowflake.connectors.application.integration;

import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.GLOBAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses;
import com.snowflake.connectors.application.ingestion.definition.InMemoryResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.IngestionStrategy;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses;
import com.snowflake.connectors.common.assertions.UUIDAssertions;
import com.snowflake.connectors.taskreactor.queue.InMemoryWorkItemQueue;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TaskReactorOnIngestionScheduledCallbackTest {
  private IngestionProcessRepository ingestionProcessRepository;
  private ResourceIngestionDefinitionRepository<CustomResourceClasses.CustomResource>
      resourceIngestionDefinitionRepository;
  private WorkItemQueue workItemQueue;
  private TaskReactorOnIngestionScheduledCallback<
          CustomResourceClasses.CustomResourceId,
          CustomResourceClasses.CustomResourceMetadata,
          CustomResourceClasses.CustomIngestionConfiguration,
          CustomResourceClasses.Destination,
          CustomResourceClasses.CustomResource>
      taskReactorOnIngestionScheduledCallback;

  @BeforeEach
  void setUp() {
    this.ingestionProcessRepository = new InMemoryIngestionProcessRepository();
    this.resourceIngestionDefinitionRepository =
        new InMemoryResourceIngestionDefinitionRepository<>();
    this.workItemQueue = new InMemoryWorkItemQueue();
    this.taskReactorOnIngestionScheduledCallback =
        new TaskReactorOnIngestionScheduledCallback<>(
            ingestionProcessRepository, resourceIngestionDefinitionRepository, workItemQueue);
  }

  @Test
  void shouldCreateWorkItem() {
    // given
    String processId =
        ingestionProcessRepository.createProcess(
            "resourceIngestionDefinitionId",
            "ingestionConfigurationId",
            "simple_process",
            IngestionProcessStatuses.SCHEDULED,
            new Variant(Map.of("key", "value")));
    resourceIngestionDefinitionRepository.save(
        new CustomResourceClasses.CustomResource(
            "resourceIngestionDefinitionId",
            "resourceName",
            true,
            "parentId",
            new CustomResourceClasses.CustomResourceId("resourceId", 2),
            new CustomResourceClasses.CustomResourceMetadata("metadataField", 333),
            List.of(
                new IngestionConfiguration<>(
                    "ingestionConfigurationId",
                    IngestionStrategy.INCREMENTAL,
                    new CustomResourceClasses.CustomIngestionConfiguration("prop1"),
                    GLOBAL,
                    null,
                    new CustomResourceClasses.Destination("destination")))));

    // when
    taskReactorOnIngestionScheduledCallback.onIngestionScheduled(
        Collections.singletonList(processId));

    // then
    List<QueueItem> workItem = workItemQueue.fetchNotProcessedAndCancelingItems();
    assertThat(workItem)
        .hasSize(1)
        .first()
        .satisfies(item -> UUIDAssertions.assertIsUUID(item.id))
        .satisfies(item -> assertThat(item.resourceId).isEqualTo(processId))
        .satisfies(item -> assertThat(item.cancelOngoingExecution).isEqualTo(false))
        .satisfies(item -> assertThat(item.timestamp).isNotNull())
        .extracting(item -> item.workerPayload.asMap())
        .extracting(
            item -> item.get("id").asString(),
            item -> item.get("processId").asString(),
            item -> item.get("resourceIngestionDefinitionId").asString(),
            item -> item.get("ingestionConfigurationId").asString(),
            item -> item.get("resourceId"),
            item -> item.get("ingestionStrategy").asString(),
            item -> item.get("resourceMetadata"),
            item -> item.get("destination"),
            item -> item.get("customConfig"),
            item -> item.get("metadata"))
        .contains(
            processId,
            processId,
            "resourceIngestionDefinitionId",
            "ingestionConfigurationId",
            new Variant(Map.of("property1", "resourceId", "property2", 2)),
            IngestionStrategy.INCREMENTAL.toString(),
            new Variant(Map.of("field1", "metadataField", "field2", 333)),
            new Variant(Map.of("field", "destination")),
            new Variant(Map.of("property1", "prop1")),
            new Variant(Map.of("key", "value")));
  }

  @Test
  void shouldThrowWhenProcessDoesntExist() {
    // expect
    assertThatThrownBy(
            () ->
                taskReactorOnIngestionScheduledCallback.onIngestionScheduled(
                    Collections.singletonList("some_processId")))
        .isInstanceOf(InvalidIngestionProcessException.class)
        .hasMessageContaining("Ingestion Process with id 'some_processId' does not exist");
  }

  @Test
  void shouldThrowWhenResourceDoesntExist() {
    // given
    String processId =
        ingestionProcessRepository.createProcess(
            "some_resourceIngestionDefinitionId",
            "ingestionConfigurationId",
            "simple_process",
            IngestionProcessStatuses.SCHEDULED,
            new Variant(Map.of("key", "value")));

    // expect
    assertThatThrownBy(
            () ->
                taskReactorOnIngestionScheduledCallback.onIngestionScheduled(
                    Collections.singletonList(processId)))
        .isInstanceOf(InvalidIngestionProcessException.class)
        .hasMessageContaining(
            "Resource Ingestion Definition with id 'some_resourceIngestionDefinitionId' does not"
                + " exist");
  }

  @Test
  void shouldThrowWhenIngestionConfigurationDoesntExist() {
    // given
    String processId =
        ingestionProcessRepository.createProcess(
            "resourceIngestionDefinitionId",
            "some_ingestionConfigurationId",
            "simple_process",
            IngestionProcessStatuses.SCHEDULED,
            new Variant(Map.of("key", "value")));
    resourceIngestionDefinitionRepository.save(
        new CustomResourceClasses.CustomResource(
            "resourceIngestionDefinitionId",
            "resourceName",
            true,
            "parentId",
            new CustomResourceClasses.CustomResourceId("resourceId", 2),
            new CustomResourceClasses.CustomResourceMetadata("metadataField", 333),
            List.of(
                new IngestionConfiguration<>(
                    "ingestionConfigurationId",
                    IngestionStrategy.INCREMENTAL,
                    new CustomResourceClasses.CustomIngestionConfiguration("prop1"),
                    GLOBAL,
                    null,
                    new CustomResourceClasses.Destination("destination")))));

    // expect
    assertThatThrownBy(
            () ->
                taskReactorOnIngestionScheduledCallback.onIngestionScheduled(
                    Collections.singletonList(processId)))
        .isInstanceOf(InvalidIngestionProcessException.class)
        .hasMessageContaining(
            "Ingestion Configuration with id 'some_ingestionConfigurationId' is not defined");
  }
}
