/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.ingestion;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link IngestionProcess}. */
public class IngestionProcessAssert
    extends AbstractAssert<IngestionProcessAssert, IngestionProcess> {

  /**
   * Creates a new {@link IngestionProcessAssert}.
   *
   * @param ingestionProcess asserted ingestion process
   * @param selfType self type
   */
  public IngestionProcessAssert(
      IngestionProcess ingestionProcess, Class<IngestionProcessAssert> selfType) {
    super(ingestionProcess, selfType);
  }

  /**
   * Asserts that this ingestion process has an id equal to the specified value.
   *
   * @param id expected id
   * @return this assertion
   */
  public IngestionProcessAssert hasId(String id) {
    assertThat(actual.getId()).isEqualTo(id);
    return this;
  }

  /**
   * Asserts that this ingestion process has a resource ingestion definition id equal to the
   * specified value.
   *
   * @param resourceIngestionDefinitionId expected resource ingestion definition id
   * @return this assertion
   */
  public IngestionProcessAssert hasResourceIngestionDefinitionId(
      String resourceIngestionDefinitionId) {
    assertThat(actual.getResourceIngestionDefinitionId()).isEqualTo(resourceIngestionDefinitionId);
    return this;
  }

  /**
   * Asserts that this ingestion process has an ingestion configuration id equal to the specified
   * value.
   *
   * @param ingestionConfigurationId expected ingestion configuration id
   * @return this assertion
   */
  public IngestionProcessAssert hasIngestionConfigurationId(String ingestionConfigurationId) {
    assertThat(actual.getIngestionConfigurationId()).isEqualTo(ingestionConfigurationId);
    return this;
  }

  /**
   * Asserts that this ingestion process has a type equal to the specified value.
   *
   * @param type expected type
   * @return this assertion
   */
  public IngestionProcessAssert hasType(String type) {
    assertThat(actual.getType()).isEqualTo(type);
    return this;
  }

  /**
   * Asserts that this ingestion process has a status equal to the specified value.
   *
   * @param status expected status
   * @return this assertion
   */
  public IngestionProcessAssert hasStatus(String status) {
    assertThat(actual.getStatus()).isEqualTo(status);
    return this;
  }

  /**
   * Asserts that this ingestion process has a createdAt timestamp equal to the specified value.
   *
   * @param createdAt expected createdAt timestamp
   * @return this assertion
   */
  public IngestionProcessAssert hasCreatedAt(Instant createdAt) {
    assertThat(actual.getCreatedAt()).isEqualTo(createdAt.truncatedTo(ChronoUnit.MILLIS));
    return this;
  }

  /**
   * Asserts that this ingestion process has a finishedAt timestamp equal to the specified value.
   *
   * @param finishedAt expected finishedAt timestamp
   * @return this assertion
   */
  public IngestionProcessAssert hasFinishedAt(Instant finishedAt) {
    assertThat(actual.getFinishedAt()).isEqualTo(finishedAt.truncatedTo(ChronoUnit.MILLIS));
    return this;
  }

  /**
   * Asserts that null-check result for finishedAt timestamp of this ingestion process is equal to
   * the specified value.
   *
   * @param nullCheckResult expected null-check result
   * @return this assertion
   */
  public IngestionProcessAssert hasFinishedAtNull(boolean nullCheckResult) {
    assertThat(actual.getFinishedAt() == null).isEqualTo(nullCheckResult);
    return this;
  }

  /**
   * Asserts that this ingestion process has a metadata equal to the specified value.
   *
   * @param metadata expected metadata
   * @return this assertion
   */
  public IngestionProcessAssert hasMetadata(Variant metadata) {
    if (metadata == null) {
      assertThat(actual.getMetadata()).isNull();
    } else {
      assertThat(actual.getMetadata()).isNotNull();
      assertThat(actual.getMetadata().asString()).isEqualTo(metadata.asString());
    }
    return this;
  }
}
