/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.integration;

import static com.snowflake.connectors.application.integration.InvalidIngestionProcessException.ingestionConfigurationDoesNotExist;
import static com.snowflake.connectors.application.integration.InvalidIngestionProcessException.processDoesNotExist;
import static com.snowflake.connectors.application.integration.InvalidIngestionProcessException.resourceIngestionDefinitionDoesNotExist;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.asVariant;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepositoryFactory;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.application.scheduler.OnIngestionScheduledCallback;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Task reactor compatible implementation of {@link OnIngestionScheduledCallback}. */
public class SchedulerTaskReactorOnIngestionScheduled implements OnIngestionScheduledCallback {

  private final IngestionProcessRepository ingestionProcessRepository;
  private final ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private final Session session;
  private final Identifier taskReactorName;

  /**
   * Creates a new {@link SchedulerTaskReactorOnIngestionScheduled}.
   *
   * @param ingestionProcessRepository ingestion process repository
   * @param resourceIngestionDefinitionRepository resource ingestion definition repository
   * @param session Snowpark session object
   * @param taskReactorName task reactor instance name
   */
  public SchedulerTaskReactorOnIngestionScheduled(
      IngestionProcessRepository ingestionProcessRepository,
      ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository,
      Session session,
      Identifier taskReactorName) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    this.session = session;
    this.taskReactorName = taskReactorName;
  }

  @Override
  public void onIngestionScheduled(String processId) {
    var process =
        ingestionProcessRepository
            .fetch(processId)
            .orElseThrow(() -> processDoesNotExist(processId));
    var resourceIngestionDefinition =
        resourceIngestionDefinitionRepository
            .fetch(process.getResourceIngestionDefinitionId())
            .orElseThrow(
                () ->
                    resourceIngestionDefinitionDoesNotExist(
                        processId, process.getResourceIngestionDefinitionId()));
    var ingestionConfig =
        selectIngestionConfig(resourceIngestionDefinition, process.getIngestionConfigurationId())
            .orElseThrow(
                () ->
                    ingestionConfigurationDoesNotExist(
                        processId,
                        resourceIngestionDefinition.getId(),
                        process.getIngestionConfigurationId()));

    Variant workItem = createWorkItemPayload(process, resourceIngestionDefinition, ingestionConfig);
    insertItemIntoTaskReactorQueue(processId, workItem);
  }

  private Optional<IngestionConfiguration<Variant, Variant>> selectIngestionConfig(
      VariantResource resource, String ingestionConfigurationId) {
    return resource.getIngestionConfigurations().stream()
        .filter(ingestionConfig -> ingestionConfigurationId.equals(ingestionConfig.getId()))
        .findAny();
  }

  private Variant createWorkItemPayload(
      IngestionProcess ingestionProcess,
      VariantResource resource,
      IngestionConfiguration<Variant, Variant> ingestionConfiguration) {
    Map<String, Variant> map = new HashMap<>();
    map.put("id", new Variant(ingestionProcess.getId()));
    map.put("resourceIngestionDefinitionId", new Variant(resource.getId()));
    map.put("ingestionConfigurationId", new Variant(ingestionConfiguration.getId()));
    map.put("resourceId", resource.getResourceId());
    map.put("ingestionStrategy", new Variant(ingestionConfiguration.getIngestionStrategy().name()));

    if (ingestionConfiguration.getDestination() != null) {
      map.put("destination", ingestionConfiguration.getDestination());
    }
    if (ingestionConfiguration.getCustomIngestionConfiguration() != null) {
      map.put("customConfig", ingestionConfiguration.getCustomIngestionConfiguration());
    }
    if (ingestionProcess.getMetadata() != null) {
      map.put("metadata", ingestionProcess.getMetadata());
    }
    return new Variant(map);
  }

  private void insertItemIntoTaskReactorQueue(String processId, Variant payload) {
    String query =
        String.format(
            "INSERT INTO %s.QUEUE (RESOURCE_ID, WORKER_PAYLOAD) SELECT %s, %s",
            taskReactorName.getValue(), asVarchar(processId), asVariant(payload));
    session.sql(query).collect();
  }

  /**
   * Returns a new instance of the default callback implementation.
   *
   * <p>Default implementation of the creator uses:
   *
   * <ul>
   *   <li>a default implementation of {@link IngestionProcessRepository}
   *   <li>a default implementation of {@link ResourceIngestionDefinitionRepository}
   * </ul>
   *
   * @param session Snowpark session object
   * @param taskReactorName task reactor instance name
   * @return a new callback instance
   */
  public static SchedulerTaskReactorOnIngestionScheduled getInstance(
      Session session, Identifier taskReactorName) {
    var ingestionProcessRepository = new DefaultIngestionProcessRepository(session);
    var resourceIngestionDefinitionRepository =
        ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
    return new SchedulerTaskReactorOnIngestionScheduled(
        ingestionProcessRepository,
        resourceIngestionDefinitionRepository,
        session,
        taskReactorName);
  }
}
