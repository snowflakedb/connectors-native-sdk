/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Objects;

/** Representation of an ingestion process. */
public class IngestionProcess {

  private final String id;
  private final String resourceIngestionDefinitionId;
  private final String ingestionConfigurationId;
  private final String type;
  private final String status;
  private final Instant createdAt;
  private final Instant finishedAt;
  private final Variant metadata;

  /**
   * Creates a new {@link IngestionProcess}.
   *
   * @param id ingestion process id
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param type process type
   * @param status {@link IngestionProcessStatuses process status}
   * @param createdAt timestamp of the process creation
   * @param finishedAt timestamp of the process end
   * @param metadata additional process metadata
   */
  public IngestionProcess(
      String id,
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status,
      Instant createdAt,
      Instant finishedAt,
      Variant metadata) {
    this.id = id;
    this.resourceIngestionDefinitionId = resourceIngestionDefinitionId;
    this.ingestionConfigurationId = ingestionConfigurationId;
    this.type = type;
    this.status = status;
    this.createdAt = createdAt;
    this.finishedAt = finishedAt;
    this.metadata = metadata;
  }

  /**
   * Returns the id of this ingestion process.
   *
   * @return id of this ingestion process
   */
  public String getId() {
    return id;
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
   * Returns the process type.
   *
   * @return process type
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the process status.
   *
   * @return process status.
   */
  public String getStatus() {
    return status;
  }

  /**
   * Returns the timestamp of the process creation.
   *
   * @return timestamp of the process creation
   */
  public Instant getCreatedAt() {
    return createdAt;
  }

  /**
   * Returns the timestamp of the process end.
   *
   * @return timestamp of the process end
   */
  public Instant getFinishedAt() {
    return finishedAt;
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
   * Returns a copy of this process with the specified status set.
   *
   * @param newStatus {@link IngestionProcessStatuses new process status}
   * @return a copy of this process with the specified status set
   */
  public IngestionProcess withStatus(String newStatus) {
    return new IngestionProcess(
        id,
        resourceIngestionDefinitionId,
        ingestionConfigurationId,
        type,
        newStatus,
        createdAt,
        finishedAt,
        metadata);
  }

  /**
   * Returns a copy of this process with the specified metadata set.
   *
   * @param newMetadata new process metadata
   * @return a copy of this process with the specified metadata set
   */
  public IngestionProcess withMetadata(Variant newMetadata) {
    return new IngestionProcess(
        id,
        resourceIngestionDefinitionId,
        ingestionConfigurationId,
        type,
        status,
        createdAt,
        finishedAt,
        newMetadata);
  }

  /**
   * Returns a copy of this process with the specified finished at.
   *
   * @param newFinishedAt new process finished at date
   * @return a copy of this process with the specified finished at
   */
  public IngestionProcess withFinishedAt(Instant newFinishedAt) {
    return new IngestionProcess(
        id,
        resourceIngestionDefinitionId,
        ingestionConfigurationId,
        type,
        status,
        createdAt,
        newFinishedAt,
        metadata);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IngestionProcess that = (IngestionProcess) o;
    return Objects.equals(id, that.id)
        && Objects.equals(resourceIngestionDefinitionId, that.resourceIngestionDefinitionId)
        && Objects.equals(ingestionConfigurationId, that.ingestionConfigurationId)
        && Objects.equals(type, that.type)
        && Objects.equals(status, that.status)
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(finishedAt, that.finishedAt)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        resourceIngestionDefinitionId,
        ingestionConfigurationId,
        type,
        status,
        createdAt,
        finishedAt,
        metadata);
  }
}
