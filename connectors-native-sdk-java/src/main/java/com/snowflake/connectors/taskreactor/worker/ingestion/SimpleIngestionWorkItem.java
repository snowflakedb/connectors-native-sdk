/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import com.fasterxml.jackson.databind.JavaType;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.util.variant.VariantMapper;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A payload of a work item containing a data needed by a single simple ingestion task. Parameters
 * are taken from the @ResourceIngestionDefinition class implementation.
 *
 * @param <I> resource id class, containing properties which identify the resource in the source
 *     system
 * @param <M> resource metadata class, containing additional properties which identify the resource
 *     in the source system. The properties can be fetched automatically or calculated by the
 *     connector
 * @param <C> custom ingestion configuration class, containing custom ingestion properties
 * @param <D> destination configuration class, containing properties describing where the ingested
 *     data should be stored
 */
public class SimpleIngestionWorkItem<I, M, C, D> {

  @Deprecated private String id;
  private String processId;
  private String processType;
  private String resourceIngestionDefinitionId;
  private String ingestionConfigurationId;
  private I resourceId;
  private String ingestionStrategy;
  private M resourceMetadata;
  private C customConfig;
  private D destination;
  private Variant metadata;

  /**
   * Creates an empty {@link SimpleIngestionWorkItem}.
   *
   * <p>This constructor is used by the reflection-based mapping process and should not be used for
   * any other purpose.
   */
  public SimpleIngestionWorkItem() {}

  /**
   * Creates a new {@link SimpleIngestionWorkItem}.
   *
   * @param id ingestion process id - deprecated, use processId instead
   * @param processId ingestion process id
   * @param processType ingestion process type
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param resourceId properties which identify the resource in the source system
   * @param ingestionStrategy ingestion strategy
   * @param resourceMetadata resource metadata
   * @param customConfig custom ingestion properties
   * @param destination properties describing where the ingested data should be stored
   * @param metadata process metadata
   */
  public SimpleIngestionWorkItem(
      @Deprecated String id,
      String processId,
      String processType,
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      I resourceId,
      String ingestionStrategy,
      M resourceMetadata,
      C customConfig,
      D destination,
      Variant metadata) {
    this.id = id;
    this.processId = processId;
    this.processType = processType;
    this.resourceIngestionDefinitionId = resourceIngestionDefinitionId;
    this.ingestionConfigurationId = ingestionConfigurationId;
    this.resourceId = resourceId;
    this.ingestionStrategy = ingestionStrategy;
    this.resourceMetadata = resourceMetadata;
    this.customConfig = customConfig;
    this.destination = destination;
    this.metadata = metadata;
  }

  /**
   * Returns the id of the ingestion process.
   *
   * @return id of the ingestion process
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the ingestion process id.
   *
   * @return ingestion process id
   */
  public String getProcessId() {
    return id;
  }

  /**
   * Returns the ingestion process type.
   *
   * @return ingestion process type
   */
  public String getProcessType() {
    return processType;
  }

  /**
   * Returns the resource ingestion definition id.
   *
   * @return resource ingestion definition id
   */
  public String getResourceIngestionDefinitionId() {
    return resourceIngestionDefinitionId;
  }

  /**
   * Returns the ingestion configuration id.
   *
   * @return ingestion configuration id
   */
  public String getIngestionConfigurationId() {
    return ingestionConfigurationId;
  }

  /**
   * Returns the resource id.
   *
   * @return resource id
   */
  public I getResourceId() {
    return resourceId;
  }

  /**
   * Returns the ingestion strategy.
   *
   * @return ingestion strategy
   */
  public String getIngestionStrategy() {
    return ingestionStrategy;
  }

  /**
   * Returns the resource metadata.
   *
   * @return resource metadata
   */
  public M getResourceMetadata() {
    return resourceMetadata;
  }

  /**
   * Returns the destination.
   *
   * @return destination
   */
  public D getDestination() {
    return destination;
  }

  /**
   * Returns the custom configuration.
   *
   * @return custom configuration
   */
  public C getCustomConfig() {
    return customConfig;
  }

  /**
   * Returns the process metadata.
   *
   * @return process metadata
   */
  public Variant getMetadata() {
    return metadata;
  }

  /**
   * Creates a new {@link SimpleIngestionWorkItem} from a {@link WorkItem}.
   *
   * @param workItem work item
   * @param simpleIngestionWorkItemType type of the {@link SimpleIngestionWorkItem}
   * @param <I> resource id class, containing properties which identify the resource in the source
   *     system
   * @param <M> resource metadata class, containing additional properties which identify the
   *     resource in the source system. The properties can be fetched automatically or calculated by
   *     the connector
   * @param <C> custom ingestion configuration class, containing custom ingestion properties
   * @param <D> destination configuration class, containing properties describing where the ingested
   *     data should be stored
   * @return new {@link SimpleIngestionWorkItem}
   */
  static <I, M, D, C> SimpleIngestionWorkItem<I, M, D, C> from(
      WorkItem workItem, JavaType simpleIngestionWorkItemType) {
    return VariantMapper.mapVariant(workItem.payload, simpleIngestionWorkItemType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SimpleIngestionWorkItem<?, ?, ?, ?> that = (SimpleIngestionWorkItem<?, ?, ?, ?>) o;
    return Objects.equals(id, that.id)
        && Objects.equals(processId, that.processId)
        && Objects.equals(processType, that.processType)
        && Objects.equals(resourceIngestionDefinitionId, that.resourceIngestionDefinitionId)
        && Objects.equals(ingestionConfigurationId, that.ingestionConfigurationId)
        && Objects.equals(resourceId, that.resourceId)
        && Objects.equals(ingestionStrategy, that.ingestionStrategy)
        && Objects.equals(resourceMetadata, that.resourceMetadata)
        && Objects.equals(customConfig, that.customConfig)
        && Objects.equals(destination, that.destination)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        processId,
        processType,
        resourceIngestionDefinitionId,
        ingestionConfigurationId,
        resourceId,
        ingestionStrategy,
        resourceMetadata,
        customConfig,
        destination,
        metadata);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SimpleIngestionWorkItem.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("processId='" + processId + "'")
        .add("processType='" + processType + "'")
        .add("resourceIngestionDefinitionId='" + resourceIngestionDefinitionId + "'")
        .add("ingestionConfigurationId='" + ingestionConfigurationId + "'")
        .add("resourceId=" + resourceId)
        .add("ingestionStrategy='" + ingestionStrategy + "'")
        .add("resourceMetadata=" + resourceMetadata)
        .add("customConfig=" + customConfig)
        .add("destination=" + destination)
        .add("metadata=" + metadata)
        .toString();
  }
}
