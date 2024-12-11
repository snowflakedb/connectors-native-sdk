package com.snowflake.connectors.common.assertions.ingestion;

import static com.snowflake.connectors.common.assertions.UUIDAssertions.assertIsUUID;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.IngestionStrategy;
import com.snowflake.connectors.application.ingestion.definition.ScheduleType;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

/** AssertJ based assertions for {@link IngestionConfiguration}. */
public class IngestionConfigurationAssert<C, D>
    extends AbstractAssert<IngestionConfigurationAssert<C, D>, IngestionConfiguration<C, D>> {

  /**
   * Creates a new {@link IngestionConfigurationAssert}
   *
   * @param configuration asserted ingestion configuration
   * @param selfType self type
   */
  public IngestionConfigurationAssert(
      IngestionConfiguration<C, D> configuration, Class<IngestionConfigurationAssert> selfType) {
    super(configuration, selfType);
  }

  /**
   * Asserts that this ingestion configuration has an id matching UUID.
   *
   * @return this assertion
   */
  public IngestionConfigurationAssert<C, D> hasIdAsUUID() {
    assertIsUUID(this.actual.getId());
    return this;
  }

  /**
   * Asserts that this ingestion configuration has a destination equal to the specified value.
   *
   * @param destination expected destination
   * @return this assertion
   */
  public IngestionConfigurationAssert<C, D> hasDestination(D destination) {
    Assertions.assertThat(this.actual.getDestination()).isEqualTo(destination);
    return this;
  }

  /**
   * Asserts that this ingestion configuration has schedule definition equal to null.
   *
   * @return this assertion
   */
  public IngestionConfigurationAssert<C, D> hasNullScheduleDefinition() {
    Assertions.assertThat(this.actual.getScheduleDefinition()).isNull();
    return this;
  }

  /**
   * Asserts that this ingestion configuration has an ingestion strategy equal to the specified
   * value.
   *
   * @param ingestionStrategy expected ingestion strategy
   * @return this assertion
   */
  public IngestionConfigurationAssert<C, D> hasIngestionStrategy(
      IngestionStrategy ingestionStrategy) {
    Assertions.assertThat(this.actual.getIngestionStrategy()).isEqualTo(ingestionStrategy);
    return this;
  }

  /**
   * Asserts that this ingestion configuration has a schedule type equal to the specified value.
   *
   * @param scheduleType expected schedule type
   * @return this assertion
   */
  public IngestionConfigurationAssert<C, D> hasScheduleType(ScheduleType scheduleType) {
    Assertions.assertThat(this.actual.getScheduleType()).isEqualTo(scheduleType);
    return this;
  }
}
