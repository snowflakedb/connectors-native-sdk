/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import com.snowflake.connectors.application.status.exception.InvalidConnectorConfigurationStatusException;
import com.snowflake.connectors.application.status.exception.InvalidConnectorStatusException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Representation of the full connector status, comprised of the connector and connector
 * configuration statuses.
 */
public class FullConnectorStatus {

  private ConnectorStatus status;
  private ConnectorStatus.ConnectorConfigurationStatus configurationStatus;

  /**
   * Creates an empty {@link FullConnectorStatus}.
   *
   * <p>This constructor is used by the Jackson JSON deserialization process and should not be used
   * for any other purpose.
   */
  public FullConnectorStatus() {}

  /**
   * Creates a new {@link FullConnectorStatus}.
   *
   * @param status connector status
   * @param configurationStatus connector configuration status
   */
  public FullConnectorStatus(
      ConnectorStatus status, ConnectorStatus.ConnectorConfigurationStatus configurationStatus) {
    this.status = status;
    this.configurationStatus = configurationStatus;
  }

  /**
   * Validates whether this connector status is in provided ones.
   *
   * @param expected expected connector statuses
   * @throws InvalidConnectorStatusException if this connector status is different from the expected
   *     ones.
   */
  public void validateConnectorStatusIn(ConnectorStatus... expected) {
    if (!hasStatus(expected)) {
      throw new InvalidConnectorStatusException(this.status, expected);
    }
  }

  /**
   * Validates whether this connector configuration status is present among the provided statuses.
   *
   * @param expectedStatuses expected connector configuration statuses
   * @throws InvalidConnectorStatusException if this connector status is not present among the
   *     provided statuses
   */
  public void validateConfigurationStatusIsIn(
      ConnectorStatus.ConnectorConfigurationStatus... expectedStatuses) {
    if (!Arrays.asList(expectedStatuses).contains(this.configurationStatus)) {
      throw new InvalidConnectorConfigurationStatusException(
          this.configurationStatus, expectedStatuses);
    }
  }

  /**
   * Validates whether this connector configuration status is not present among the provided
   * statuses.
   *
   * @param unexpectedStatuses unexpected connector configuration statuses
   * @throws InvalidConnectorStatusException if this connector status is present among the provided
   *     statuses
   */
  public void validateConfigurationStatusIsNotIn(
      ConnectorStatus.ConnectorConfigurationStatus... unexpectedStatuses) {
    if (Arrays.asList(unexpectedStatuses).contains(this.configurationStatus)) {
      throw new InvalidConnectorConfigurationStatusException(
          this.configurationStatus,
          ConnectorStatus.ConnectorConfigurationStatus.getStatusesExcluding(unexpectedStatuses));
    }
  }

  /**
   * Returns this connector status.
   *
   * @return connector status
   */
  public ConnectorStatus getStatus() {
    return status;
  }

  /**
   * Sets this connector status.
   *
   * @param status connector status
   */
  public void setStatus(ConnectorStatus status) {
    this.status = status;
  }

  /**
   * Returns this connector configuration status.
   *
   * @return connector configuration status
   */
  public ConnectorStatus.ConnectorConfigurationStatus getConfigurationStatus() {
    return configurationStatus;
  }

  /**
   * Sets this connector configuration status.
   *
   * @param configurationStatus connector configuration status
   */
  public void setConfigurationStatus(
      ConnectorStatus.ConnectorConfigurationStatus configurationStatus) {
    this.configurationStatus = configurationStatus;
  }

  /**
   * Returns whether this connector status contains the provided ones.
   *
   * @param expected expected connector statuses
   * @return whether this connector status is equal to the provided one
   */
  public boolean hasStatus(ConnectorStatus... expected) {
    return Arrays.asList(expected).contains(status);
  }

  /**
   * Returns whether this connector configuration status is equal to the provided one.
   *
   * @param expected expected connector configuration status
   * @return whether this connector configuration status is equal to the provided one
   */
  public boolean hasConfigurationStatus(ConnectorStatus.ConnectorConfigurationStatus expected) {
    return configurationStatus == expected;
  }

  /**
   * Returns whether this connector configuration status is equal to one of provided in the
   * argument.
   *
   * @param expected expected connector configuration statuses
   * @return whether this connector configuration status is to one of the provided ones
   */
  public boolean hasConfigurationStatus(ConnectorStatus.ConnectorConfigurationStatus... expected) {
    return Arrays.asList(expected).contains(configurationStatus);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FullConnectorStatus that = (FullConnectorStatus) o;
    return Objects.equals(status, that.status)
        && Objects.equals(configurationStatus, that.configurationStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, configurationStatus);
  }
}
