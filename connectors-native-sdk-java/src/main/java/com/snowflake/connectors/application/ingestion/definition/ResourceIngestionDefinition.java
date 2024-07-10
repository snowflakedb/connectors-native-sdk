/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import java.util.List;
import java.util.Objects;

/**
 * Base class of a resource ingestion definition. It contains information about a resource present
 * in a source system, and information about how the resource should be ingested.
 *
 * <p>Implementation of this class has to meet the following criteria:
 *
 * <ul>
 *   <li>must contain a public, no-args constructor, which will be used during reflection-based
 *       deserialization process
 *   <li>type parameter classes must contain a public, no-args constructor, public getters, and
 *       setters which will be used during reflection-based serialization and deserialization
 *       processes
 * </ul>
 *
 * <p>A specific, uniquely identifying resource id class must always be specified. All other type
 * parameters are used for additional, non-obligatory data and thus may be substituted by the {@link
 * EmptyImplementation} class if not needed.
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
public abstract class ResourceIngestionDefinition<I, M, C, D> {

  private String id;
  private String name;
  private boolean enabled;
  private String parentId;
  private I resourceId;
  private M resourceMetadata;
  private List<IngestionConfiguration<C, D>> ingestionConfigurations;

  /**
   * Creates an empty {@link ResourceIngestionDefinition}.
   *
   * <p>This constructor is used by the reflection-based mapping process and should not be used for
   * any other purpose.
   */
  public ResourceIngestionDefinition() {}

  /**
   * Creates a new {@link ResourceIngestionDefinition}.
   *
   * @param id resource ingestion definition id
   * @param name resource name
   * @param enabled should the ingestion for the resource be enabled
   * @param parentId id of the parent definition
   * @param resourceId properties which identify the resource in the source system
   * @param resourceMetadata resource metadata
   * @param ingestionConfigurations ingestion configurations
   */
  public ResourceIngestionDefinition(
      String id,
      String name,
      boolean enabled,
      String parentId,
      I resourceId,
      M resourceMetadata,
      List<IngestionConfiguration<C, D>> ingestionConfigurations) {
    this.id = id;
    this.name = name;
    this.enabled = enabled;
    this.parentId = parentId;
    this.resourceId = resourceId;
    this.resourceMetadata = resourceMetadata;
    this.ingestionConfigurations = ingestionConfigurations;
  }

  /**
   * Returns the resource ingestion definition id.
   *
   * @return resource ingestion definition id
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the resource name.
   *
   * @return resource name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns whether the ingestion for the resource is enabled.
   *
   * @return whether the ingestion for the resource is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns whether the ingestion for the resource is disabled.
   *
   * @return whether the ingestion for the resource is disabled
   */
  public boolean isDisabled() {
    return !enabled;
  }

  /**
   * Returns the id of the parent definition.
   *
   * @return id of the parent definition
   */
  public String getParentId() {
    return parentId;
  }

  /**
   * Returns the properties which identify the resource in the source system.
   *
   * @return properties which identify the resource in the source system
   */
  public I getResourceId() {
    return resourceId;
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
   * Returns the ingestion configurations.
   *
   * @return ingestion configurations
   */
  public List<IngestionConfiguration<C, D>> getIngestionConfigurations() {
    return ingestionConfigurations;
  }

  /**
   * Sets the resource ingestion definition id.
   *
   * @param id resource ingestion definition id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Sets the resource name.
   *
   * @param name resource name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets whether the ingestion for the resource is enabled.
   *
   * @param enabled whether the ingestion for the resource is enabled
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /** Sets enabled flag to true */
  public void setEnabled() {
    this.enabled = true;
  }

  /** Sets enabled flag to false */
  public void setDisabled() {
    this.enabled = false;
  }

  /**
   * Sets the id of the parent definition.
   *
   * @param parentId id of the parent definition
   */
  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  /**
   * Sets the properties which identify the resource in the source system.
   *
   * @param resourceId properties which identify the resource in the source system
   */
  public void setResourceId(I resourceId) {
    this.resourceId = resourceId;
  }

  /**
   * Sets the resource metadata
   *
   * @param resourceMetadata resource metadata
   */
  public void setResourceMetadata(M resourceMetadata) {
    this.resourceMetadata = resourceMetadata;
  }

  /**
   * Sets the ingestion configurations.
   *
   * @param ingestionConfigurations ingestion configurations
   */
  public void setIngestionConfigurations(
      List<IngestionConfiguration<C, D>> ingestionConfigurations) {
    this.ingestionConfigurations = ingestionConfigurations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ResourceIngestionDefinition<?, ?, ?, ?> resource = (ResourceIngestionDefinition<?, ?, ?, ?>) o;
    return Objects.equals(id, resource.id)
        && Objects.equals(name, resource.name)
        && Objects.equals(enabled, resource.enabled)
        && Objects.equals(parentId, resource.parentId)
        && Objects.equals(resourceId, resource.resourceId)
        && Objects.equals(resourceMetadata, resource.resourceMetadata)
        && Objects.equals(ingestionConfigurations, resource.ingestionConfigurations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, name, enabled, parentId, resourceId, resourceMetadata, ingestionConfigurations);
  }

  @Override
  public String toString() {
    return "ResourceIngestionDefinition{"
        + "id='"
        + id
        + '\''
        + ", name='"
        + name
        + '\''
        + ", enabled="
        + enabled
        + ", parentId='"
        + parentId
        + '\''
        + ", resourceId="
        + resourceId
        + ", resourceMetadata="
        + resourceMetadata
        + ", ingestionConfigurations="
        + ingestionConfigurations
        + '}';
  }
}
