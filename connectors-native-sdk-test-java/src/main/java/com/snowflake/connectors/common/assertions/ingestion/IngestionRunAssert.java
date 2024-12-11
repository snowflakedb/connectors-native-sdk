/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.ingestion;

import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.COMPLETED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.common.assertions.UUIDAssertions.assertIsUUID;

import com.snowflake.connectors.application.observability.IngestionRun;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

/** AssertJ based assertions for {@link IngestionRun}. */
public class IngestionRunAssert extends AbstractAssert<IngestionRunAssert, IngestionRun> {

  /**
   * Creates a new {@link IngestionRunAssert}.
   *
   * @param ingestionRun asserted ingestion run
   * @param selfType self type
   */
  public IngestionRunAssert(IngestionRun ingestionRun, Class<IngestionRunAssert> selfType) {
    super(ingestionRun, selfType);
  }

  /**
   * Asserts that this ingestion run has an id matching UUID.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasIdAsUUID() {
    assertIsUUID(this.actual.getId());
    return this;
  }

  /**
   * Asserts that this ingestion run has an id equal to the specified value.
   *
   * @param id expected id
   * @return this assertion
   */
  public IngestionRunAssert hasId(String id) {
    assertThat(actual.getId()).isEqualTo(id);
    return this;
  }

  /**
   * Asserts that this ingestion run has a resource ingestion definition id equal to the specified
   * value.
   *
   * @param resourceIngestionDefinitionId expected resource ingestion definition id
   * @return this assertion
   */
  public IngestionRunAssert hasIngestionDefinitionId(String resourceIngestionDefinitionId) {
    assertThat(actual.getIngestionDefinitionId()).isEqualTo(resourceIngestionDefinitionId);
    return this;
  }

  /**
   * Asserts that this ingestion run has an ingestion configuration id equal to the specified value.
   *
   * @param ingestionConfigurationId expected ingestion configuration id
   * @return this assertion
   */
  public IngestionRunAssert hasIngestionConfigurationId(String ingestionConfigurationId) {
    assertThat(actual.getIngestionConfigurationId()).isEqualTo(ingestionConfigurationId);
    return this;
  }

  /**
   * Asserts that this ingestion run has a configuration id matching UUID.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasIngestionConfigurationIdAsUUID() {
    assertIsUUID(this.actual.getIngestionConfigurationId());
    return this;
  }

  /**
   * Asserts that this ingestion run has an ingestion process id equal to the specified value.
   *
   * @param ingestionProcessId expected ingestion process id
   * @return this assertion
   */
  public IngestionRunAssert hasIngestionProcessId(String ingestionProcessId) {
    assertThat(actual.getIngestionProcessId()).isEqualTo(ingestionProcessId);
    return this;
  }

  /**
   * Asserts that this ingestion run has a process id matching UUID.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasIngestionProcessIdAsUUID() {
    assertIsUUID(this.actual.getIngestionProcessId());
    return this;
  }

  /**
   * Asserts that this ingestion run has an ingestion status equal to the specified value.
   *
   * @param ingestionStatus expected ingestion status
   * @return this assertion
   */
  public IngestionRunAssert hasStatus(IngestionRun.IngestionStatus ingestionStatus) {
    assertThat(actual.getStatus()).isEqualTo(ingestionStatus);
    return this;
  }

  /**
   * Asserts that this ingestion run has a startedAt timestamp equal to the specified value.
   *
   * @param startedAt expected startedAt timestamp
   * @return this assertion
   */
  public IngestionRunAssert hasStartedAt(Instant startedAt) {
    assertThat(actual.getStartedAt()).isEqualTo(startedAt);
    return this;
  }

  /**
   * Asserts that ingestion run started at value is not null.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasStartedAt() {
    assertThat(this.actual.getStartedAt()).isNotNull();
    return this;
  }

  /**
   * Asserts that this ingestion run has a StartedAt timestamp between to the specified values.
   *
   * @param start expected start timestamp
   * @param end expected end timestamp
   * @return this assertion
   */
  public IngestionRunAssert hasStartedAtBetween(Instant start, Instant end) {
    assertThat(actual.getStartedAt()).isBetween(start, end);
    return this;
  }

  /**
   * Asserts that this ingestion run has a completedAt timestamp equal to the specified value.
   *
   * @param completedAt expected completedAt timestamp
   * @return this assertion
   */
  public IngestionRunAssert hasCompletedAt(Instant completedAt) {
    assertThat(actual.getCompletedAt()).isEqualTo(completedAt);
    return this;
  }

  /**
   * Asserts that ingestion run completed at value is not null.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasCompletedAt() {
    assertThat(this.actual.getCompletedAt()).isNotNull();
    return this;
  }

  /**
   * Asserts that this ingestion run has a completedAt timestamp between to the specified values.
   *
   * @param start expected start timestamp
   * @param end expected end timestamp
   * @return this assertion
   */
  public IngestionRunAssert hasCompletedAtBetween(Instant start, Instant end) {
    assertThat(actual.getCompletedAt()).isBetween(start, end);
    return this;
  }

  /**
   * Asserts that ingestion run has started before completion.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasCompletedAtAfterStartedAt() {
    assertThat(this.actual.getCompletedAt()).isAfter(this.actual.getStartedAt());
    return this;
  }

  /**
   * Asserts that this ingestion run has a number of ingested rows grater than the specified value.
   *
   * @param ingestedRows expected number of ingested rows
   * @return this assertion
   */
  public IngestionRunAssert hasIngestedRowsGreaterThan(int ingestedRows) {
    assertThat(this.actual.getIngestedRows()).isGreaterThan(ingestedRows);
    return this;
  }

  /**
   * Asserts that this ingestion run has a number of ingested rows equal to the specified value.
   *
   * @param ingestedRows expected number of ingested rows
   * @return this assertion
   */
  public IngestionRunAssert hasIngestedRows(long ingestedRows) {
    assertThat(actual.getIngestedRows()).isEqualTo(ingestedRows);
    return this;
  }

  /**
   * Asserts that ingestion run updated at value is not null.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasUpdatedAt() {
    assertThat(this.actual.getUpdatedAt()).isNotNull();
    return this;
  }

  /**
   * Asserts that this ingestion run has an updatedAt timestamp equal to the specified value.
   *
   * @param updatedAt expected updatedAt timestamp
   * @return this assertion
   */
  public IngestionRunAssert hasUpdatedAt(Instant updatedAt) {
    assertThat(actual.getUpdatedAt()).isEqualTo(updatedAt);
    return this;
  }

  /**
   * Asserts that this ingestion run has a updatedAt timestamp between to the specified values.
   *
   * @param start expected start timestamp
   * @param end expected end timestamp
   * @return this assertion
   */
  public IngestionRunAssert hasUpdatedAtBetween(Instant start, Instant end) {
    assertThat(actual.getUpdatedAt()).isBetween(start, end);
    return this;
  }

  /**
   * Asserts that ingestion run metadata value is not null.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasMetadata() {
    Assertions.assertThat(this.actual.getMetadata()).isNotNull();
    return this;
  }

  /**
   * Asserts that this ingestion run has a metadata equal to the specified value.
   *
   * @param metadata expected metadata
   * @return this assertion
   */
  public IngestionRunAssert hasMetadata(Variant metadata) {
    assertThat(actual.getMetadata()).isEqualTo(metadata);
    return this;
  }

  /**
   * Asserts that this ingestion run has ingestion run in STATE COMPLETED and ingested any rows.
   *
   * @return this assertion
   */
  public IngestionRunAssert hasCompletedState() {
    assertThat(actual)
        .hasIdAsUUID()
        .hasIngestionConfigurationIdAsUUID()
        .hasIngestionProcessIdAsUUID()
        .hasStartedAt()
        .hasUpdatedAt()
        .hasCompletedAt()
        .hasCompletedAtAfterStartedAt()
        .hasMetadata(null)
        .hasStatus(COMPLETED)
        .hasIngestedRowsGreaterThan(0);
    return this;
  }
}
