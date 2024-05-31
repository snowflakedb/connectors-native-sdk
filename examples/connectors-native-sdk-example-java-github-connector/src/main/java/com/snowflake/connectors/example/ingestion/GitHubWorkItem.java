/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import com.snowflake.connectors.example.ingestion.exception.WorkItemPayloadPropertyNotFoundException;
import com.snowflake.connectors.example.ingestion.exception.WorkItemResourceIdPropertyNotFoundException;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Optional;

/**
 * Class representing a GitHub ingestion work item record which is inserted into the task reactor
 * queue.
 *
 * <p>Fields of this class are populated using deserialized data from the payload of the task
 * reactor work item.
 */
public class GitHubWorkItem {

  private static final String ORGANIZATION_PROPERTY = "organisation_name";
  private static final String REPOSITORY_PROPERTY = "repository_name";
  private static final String RESOURCE_INGESTION_DEFINITION_ID_PROPERTY =
      "resourceIngestionDefinitionId";
  private static final String INGESTION_CONFIGURATION_ID_PROPERTY = "ingestionConfigurationId";
  private static final String RESOURCE_ID_PROPERTY = "resourceId";

  private final String organization;
  private final String repository;
  private final String processId;
  private final String resourceIngestionDefinitionId;
  private final String ingestionConfigurationId;

  public static GitHubWorkItem from(WorkItem workItem) {
    var organization = extractResourceIdProperty(workItem, ORGANIZATION_PROPERTY);
    var repository = extractResourceIdProperty(workItem, REPOSITORY_PROPERTY);
    var resourceIngestionDefinitionId =
        extractPropertyFromWorkItemPayload(workItem, RESOURCE_INGESTION_DEFINITION_ID_PROPERTY);
    var ingestionConfigurationId =
        extractPropertyFromWorkItemPayload(workItem, INGESTION_CONFIGURATION_ID_PROPERTY);
    return new GitHubWorkItem(
        organization,
        repository,
        workItem.resourceId,
        resourceIngestionDefinitionId,
        ingestionConfigurationId);
  }

  private static String extractPropertyFromWorkItemPayload(WorkItem workItem, String propertyName) {
    var properties = workItem.payload.asMap();
    return Optional.ofNullable(properties.get(propertyName))
        .map(Variant::asString)
        .orElseThrow(() -> new WorkItemPayloadPropertyNotFoundException(propertyName));
  }

  private static String extractResourceIdProperty(WorkItem workItem, String propertyName) {
    var properties = workItem.payload.asMap();
    return Optional.ofNullable(properties.get(RESOURCE_ID_PROPERTY))
        .map(Variant::asMap)
        .map(it -> it.get(propertyName))
        .map(Variant::asString)
        .orElseThrow(() -> new WorkItemResourceIdPropertyNotFoundException(propertyName));
  }

  private GitHubWorkItem(
      String organization,
      String repository,
      String processId,
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId) {
    this.organization = organization;
    this.repository = repository;
    this.processId = processId;
    this.resourceIngestionDefinitionId = resourceIngestionDefinitionId;
    this.ingestionConfigurationId = ingestionConfigurationId;
  }

  public String getOrganization() {
    return organization;
  }

  public String getRepository() {
    return repository;
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
