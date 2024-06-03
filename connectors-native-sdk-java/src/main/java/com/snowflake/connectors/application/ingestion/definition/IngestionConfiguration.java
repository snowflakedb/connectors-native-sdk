/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import java.util.Objects;

/**
 * Representation of an ingestion configuration.
 *
 * @param <C> class of the custom ingestion properties
 * @param <D> class of the properties describing where the ingested data should be stored
 */
public class IngestionConfiguration<C, D> {

  private String id;
  private IngestionStrategy ingestionStrategy;
  private C customIngestionConfiguration;
  private ScheduleType scheduleType;
  private String scheduleDefinition;
  private D destination;

  /**
   * Creates an empty {@link IngestionConfiguration}.
   *
   * <p>This constructor is used by the reflection-based mapping process and should not be used for
   * any other purpose.
   */
  public IngestionConfiguration() {}

  /**
   * Creates a new {@link IngestionConfiguration}.
   *
   * @param id ingestion configuration id
   * @param ingestionStrategy ingestion strategy
   * @param customIngestionConfiguration custom ingestion properties
   * @param scheduleType schedule type
   * @param scheduleDefinition schedule definition
   * @param destination properties describing where the ingested data should be stored
   */
  public IngestionConfiguration(
      String id,
      IngestionStrategy ingestionStrategy,
      C customIngestionConfiguration,
      ScheduleType scheduleType,
      String scheduleDefinition,
      D destination) {
    this.id = id;
    this.ingestionStrategy = ingestionStrategy;
    this.customIngestionConfiguration = customIngestionConfiguration;
    this.scheduleType = scheduleType;
    this.scheduleDefinition = scheduleDefinition;
    this.destination = destination;
  }

  /**
   * Returns the ingestion configuration id.
   *
   * @return ingestion configuration id
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the ingestion strategy.
   *
   * @return ingestion strategy
   */
  public IngestionStrategy getIngestionStrategy() {
    return ingestionStrategy;
  }

  /**
   * Returns the custom ingestion properties.
   *
   * @return custom ingestion properties
   */
  public C getCustomIngestionConfiguration() {
    return customIngestionConfiguration;
  }

  /**
   * Returns the schedule type.
   *
   * @return schedule type
   */
  public ScheduleType getScheduleType() {
    return scheduleType;
  }

  /**
   * Returns the schedule definition.
   *
   * @return schedule definition
   */
  public String getScheduleDefinition() {
    return scheduleDefinition;
  }

  /**
   * Returns the properties describing where the ingested data should be stored.
   *
   * @return properties describing where the ingested data should be stored
   */
  public D getDestination() {
    return destination;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IngestionConfiguration<?, ?> that = (IngestionConfiguration<?, ?>) o;
    return Objects.equals(id, that.id)
        && ingestionStrategy == that.ingestionStrategy
        && Objects.equals(customIngestionConfiguration, that.customIngestionConfiguration)
        && scheduleType == that.scheduleType
        && Objects.equals(scheduleDefinition, that.scheduleDefinition)
        && Objects.equals(destination, that.destination);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        ingestionStrategy,
        customIngestionConfiguration,
        scheduleType,
        scheduleDefinition,
        destination);
  }
}
