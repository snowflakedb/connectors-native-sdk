/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import com.snowflake.connectors.example.ingestion.exception.WorkItemPayloadPropertyNotFoundException;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Optional;

/**
 * Class representing a template ingestion work item record which is inserted into the task reactor
 * queue.
 *
 * <p>Fields of this class are populated using deserialized data from the payload of the task
 * reactor work item.
 */
public class TemplateWorkItem {

  private static final String RESOURCE_INGESTION_DEFINITION_ID_PROPERTY =
      "resourceIngestionDefinitionId";
  private static final String INGESTION_CONFIGURATION_ID_PROPERTY = "ingestionConfigurationId";

  private final String processId;
  private final String resourceIngestionDefinitionId;
  private final String ingestionConfigurationId;

  public static TemplateWorkItem from(WorkItem workItem) {
    var resourceIngestionDefinitionId =
        extractPropertyFromWorkItemPayload(workItem, RESOURCE_INGESTION_DEFINITION_ID_PROPERTY);
    var ingestionConfigurationId =
        extractPropertyFromWorkItemPayload(workItem, INGESTION_CONFIGURATION_ID_PROPERTY);
    return new TemplateWorkItem(
        workItem.resourceId, resourceIngestionDefinitionId, ingestionConfigurationId);
  }

  private static String extractPropertyFromWorkItemPayload(WorkItem workItem, String propertyName) {
    var properties = workItem.payload.asMap();
    return Optional.ofNullable(properties.get(propertyName))
        .map(Variant::asString)
        .orElseThrow(() -> new WorkItemPayloadPropertyNotFoundException(propertyName));
  }

  private TemplateWorkItem(
      String processId, String resourceIngestionDefinitionId, String ingestionConfigurationId) {
    this.processId = processId;
    this.resourceIngestionDefinitionId = resourceIngestionDefinitionId;
    this.ingestionConfigurationId = ingestionConfigurationId;
  }

  public String getProcessId() {
    return processId;
  }

  public String getResourceIngestionDefinitionId() {
    return resourceIngestionDefinitionId;
  }

  public String getIngestionConfigurationId() {
    return ingestionConfigurationId;
  }
}
