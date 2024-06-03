/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability;

import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Objects;

/** Representation of an ingestion run. */
public class IngestionRun {

  private final String id;
  private final String ingestionDefinitionId;
  private final String ingestionConfigurationId;
  private final String ingestionProcessId;
  private final IngestionStatus status;
  private final Instant startedAt;
  private final Instant completedAt;
  private final long ingestedRows;
  private final Instant updatedAt;
  private final Variant metadata;

  /**
   * Creates a new {@link IngestionRun}.
   *
   * @param id ingestion run id
   * @param ingestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param status ingestion status
   * @param startedAt timestamp of the ingestion run start
   * @param completedAt timestamp of the ingestion run end
   * @param ingestedRows number of ingested rows
   * @param updatedAt timestamp of the last ingestion run update
   * @param ingestionProcessId ingestion process id
   * @param metadata ingestion run metadata
   */
  public IngestionRun(
      String id,
      String ingestionDefinitionId,
      String ingestionConfigurationId,
      IngestionStatus status,
      Instant startedAt,
      Instant completedAt,
      long ingestedRows,
      Instant updatedAt,
      String ingestionProcessId,
      Variant metadata) {
    this.id = id;
    this.ingestionDefinitionId = ingestionDefinitionId;
    this.ingestionConfigurationId = ingestionConfigurationId;
    this.status = status;
    this.startedAt = startedAt;
    this.completedAt = completedAt;
    this.ingestedRows = ingestedRows;
    this.updatedAt = updatedAt;
    this.ingestionProcessId = ingestionProcessId;
    this.metadata = metadata;
  }

  /**
   * Returns the id of this ingestion run.
   *
   * @return id of this ingestion run
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the resource ingestion definition id.
   *
   * @return resource ingestion definition id
   */
  public String getIngestionDefinitionId() {
    return ingestionDefinitionId;
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
   * Returns the ingestion status.
   *
   * @return ingestion status
   */
  public IngestionStatus getStatus() {
    return status;
  }

  /**
   * Returns the timestamp of the ingestion run start.
   *
   * @return timestamp of the ingestion run start
   */
  public Instant getStartedAt() {
    return startedAt;
  }

  /**
   * Returns the timestamp of the ingestion run end.
   *
   * @return timestamp of the ingestion run end
   */
  public Instant getCompletedAt() {
    return completedAt;
  }

  /**
   * Returns the number of ingested rows.
   *
   * @return number of ingested rows
   */
  public Long getIngestedRows() {
    return ingestedRows;
  }

  /**
   * Returns the timestamp of the last ingestion run update.
   *
   * @return timestamp of the last ingestion run update
   */
  public Instant getUpdatedAt() {
    return updatedAt;
  }

  /**
   * Returns the ingestion process id.
   *
   * @return ingestion process id
   */
  public String getIngestionProcessId() {
    return ingestionProcessId;
  }

  /**
   * Returns the ingestion run metadata.
   *
   * @return ingestion run metadata
   */
  public Variant getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IngestionRun that = (IngestionRun) o;
    return Objects.equals(id, that.id)
        && Objects.equals(ingestionDefinitionId, that.ingestionDefinitionId)
        && Objects.equals(ingestionConfigurationId, that.ingestionConfigurationId)
        && Objects.equals(ingestionProcessId, that.ingestionProcessId)
        && Objects.equals(status, that.status)
        && Objects.equals(startedAt, that.startedAt)
        && Objects.equals(completedAt, that.completedAt)
        && Objects.equals(ingestedRows, that.ingestedRows)
        && Objects.equals(updatedAt, that.updatedAt)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        ingestionDefinitionId,
        ingestionConfigurationId,
        ingestionProcessId,
        status,
        startedAt,
        completedAt,
        ingestedRows,
        updatedAt,
        metadata);
  }

  /** Ingestion run status. */
  public enum IngestionStatus {

    /** Ingestion ongoing, in progress. */
    IN_PROGRESS,

    /** Ingestion successfully completed. */
    COMPLETED,

    /** Ingestion failed. */
    FAILED,

    /** Ingestion cancelled. */
    CANCELED,

    /** Ingestion resource not found in the source system. */
    NOT_FOUND
  }
}
