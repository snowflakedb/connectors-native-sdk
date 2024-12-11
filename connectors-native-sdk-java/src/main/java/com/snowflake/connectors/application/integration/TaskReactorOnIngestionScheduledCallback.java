/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.integration;

import static com.snowflake.connectors.application.integration.InvalidIngestionProcessException.ingestionConfigurationDoesNotExist;
import static com.snowflake.connectors.application.integration.InvalidIngestionProcessException.processDoesNotExist;
import static com.snowflake.connectors.application.integration.InvalidIngestionProcessException.resourceIngestionDefinitionDoesNotExist;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinition;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepositoryFactory;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.application.scheduler.OnIngestionScheduledCallback;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.connectors.taskreactor.worker.ingestion.SimpleIngestionWorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.util.variant.VariantMapper;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/** Task reactor compatible implementation of {@link OnIngestionScheduledCallback}. */
public class TaskReactorOnIngestionScheduledCallback<
        I, M, C, D, R extends ResourceIngestionDefinition<I, M, C, D>>
    implements OnIngestionScheduledCallback {

  private final IngestionProcessRepository ingestionProcessRepository;
  private final ResourceIngestionDefinitionRepository<R> resourceIngestionDefinitionRepository;
  private final WorkItemQueue workItemQueue;

  /**
   * Creates a new {@link TaskReactorOnIngestionScheduledCallback}.
   *
   * @param ingestionProcessRepository ingestion process repository
   * @param resourceIngestionDefinitionRepository resource ingestion definition repository
   * @param workItemQueue work item queue repository
   */
  public TaskReactorOnIngestionScheduledCallback(
      IngestionProcessRepository ingestionProcessRepository,
      ResourceIngestionDefinitionRepository<R> resourceIngestionDefinitionRepository,
      WorkItemQueue workItemQueue) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    this.workItemQueue = workItemQueue;
  }

  @Override
  public void onIngestionScheduled(List<String> processIds) {
    List<IngestionProcess> processes = ingestionProcessRepository.fetchAllById(processIds);
    Map<String, IngestionProcess> indexedProcesses =
        processes.stream().collect(toMap(IngestionProcess::getId, Function.identity()));

    processIds.forEach(
        id -> {
          if (!indexedProcesses.containsKey(id)) {
            throw processDoesNotExist(id);
          }
        });

    List<String> resourceIds =
        processes.stream()
            .map(IngestionProcess::getResourceIngestionDefinitionId)
            .collect(toList());
    Map<String, String> resourceIdToProcessId =
        processes.stream()
            .collect(
                toMap(IngestionProcess::getResourceIngestionDefinitionId, IngestionProcess::getId));

    List<R> resources = resourceIngestionDefinitionRepository.fetchAllById(resourceIds);
    Map<String, R> indexedResources =
        resources.stream().collect(toMap(ResourceIngestionDefinition::getId, Function.identity()));

    resourceIdToProcessId.forEach(
        (resourceId, processId) -> {
          if (!indexedResources.containsKey(resourceId)) {
            throw resourceIngestionDefinitionDoesNotExist(processId, resourceId);
          }
        });

    List<WorkItem> workItems =
        resourceIdToProcessId.entrySet().stream()
            .map(entry -> createWorkItem(entry, indexedProcesses, indexedResources))
            .collect(toList());
    workItemQueue.push(workItems);
  }

  private WorkItem createWorkItem(
      Map.Entry<String, String> entry,
      Map<String, IngestionProcess> indexedProcesses,
      Map<String, R> indexedResources) {
    var resourceId = entry.getKey();
    var processId = entry.getValue();
    var process = indexedProcesses.get(processId);
    var resource = indexedResources.get(resourceId);
    IngestionConfiguration<C, D> ingestionConfiguration =
        selectIngestionConfig(resource, process.getIngestionConfigurationId())
            .orElseThrow(
                () ->
                    ingestionConfigurationDoesNotExist(
                        processId, resource.getId(), process.getIngestionConfigurationId()));
    Variant workItemPayload = createWorkItemPayload(process, resource, ingestionConfiguration);
    return new WorkItem(UUID.randomUUID().toString(), processId, workItemPayload);
  }

  private Optional<IngestionConfiguration<C, D>> selectIngestionConfig(
      R resource, String ingestionConfigurationId) {
    return resource.getIngestionConfigurations().stream()
        .filter(ingestionConfig -> ingestionConfigurationId.equals(ingestionConfig.getId()))
        .findAny();
  }

  private Variant createWorkItemPayload(
      IngestionProcess ingestionProcess,
      R resource,
      IngestionConfiguration<C, D> ingestionConfiguration) {
    SimpleIngestionWorkItem<I, M, C, D> workItem =
        new SimpleIngestionWorkItem<>(
            ingestionProcess.getId(),
            ingestionProcess.getId(),
            ingestionProcess.getType(),
            resource.getId(),
            ingestionConfiguration.getId(),
            resource.getResourceId(),
            ingestionConfiguration.getIngestionStrategy().name(),
            resource.getResourceMetadata(),
            ingestionConfiguration.getCustomIngestionConfiguration(),
            ingestionConfiguration.getDestination(),
            ingestionProcess.getMetadata());

    return VariantMapper.mapToVariant(workItem);
  }

  /**
   * Returns a new instance of the default callback implementation.
   *
   * <p>Default implementation of the creator uses:
   *
   * <ul>
   *   <li>a default implementation of {@link IngestionProcessRepository}
   *   <li>a default implementation of {@link ResourceIngestionDefinitionRepository}
   *   <li>a default implementation of {@link WorkItemQueue}
   * </ul>
   *
   * @param session Snowpark session object
   * @param taskReactorName task reactor instance name
   * @param resourceClass class of the ingested resource definition
   * @param <I> resource id class, containing properties which identify the resource in the source
   *     system
   * @param <M> resource metadata class, containing additional properties which identify the
   *     resource in the source system. The properties can be fetched automatically or calculated by
   *     the connector
   * @param <C> custom ingestion configuration class, containing custom ingestion properties
   * @param <D> destination configuration class, containing properties describing where the ingested
   *     data should be stored
   * @param <R> ingested resource definition class
   * @return a new callback instance
   */
  public static <I, M, C, D, R extends ResourceIngestionDefinition<I, M, C, D>>
      TaskReactorOnIngestionScheduledCallback<I, M, C, D, R> getInstance(
          Session session, Identifier taskReactorName, Class<R> resourceClass) {
    var ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    var resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, resourceClass);
    return new TaskReactorOnIngestionScheduledCallback<>(
        ingestionProcessRepository,
        resourceIngestionDefinitionRepository,
        WorkItemQueue.getInstance(session, taskReactorName));
  }
}
