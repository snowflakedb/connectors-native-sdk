/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import java.util.Arrays;
import java.util.stream.Stream;

/** Connector status. */
public enum ConnectorStatus {

  /** Connector is being configured (during the wizard phase). */
  CONFIGURING,

  /** Connector is being resumed. */
  STARTING,

  /** Connector is configured and running. */
  STARTED,

  /** Connector is being paused. */
  PAUSING,

  /** Connector is paused. */
  PAUSED,

  /**
   * Connector is in an unspecified stated after encountering an irreversible error, it cannot be
   * actively used.
   */
  ERROR;

  /** Connector configuration status. */
  public enum ConnectorConfigurationStatus {

    /** Connector has just been installed. */
    INSTALLED,

    /** Prerequisites wizard phase has been completed. */
    PREREQUISITES_DONE,

    /** Connector configuration wizard phase has been completed. */
    CONFIGURED,

    /** Connection configuration wizard phase has been completed. */
    CONNECTED,

    /** Configuration finalization wizard phase has been completed. */
    FINALIZED;

    /**
     * Returns all possible configuration statuses excluding the ones provided.
     *
     * @param excludedStatuses configuration statuses to exclude
     * @return all possible configuration statuses excluding the ones provided
     */
    public static ConnectorConfigurationStatus[] getStatusesExcluding(
        ConnectorConfigurationStatus... excludedStatuses) {
      return Stream.of(ConnectorConfigurationStatus.values())
          .filter(it -> !Arrays.asList(excludedStatuses).contains(it))
          .toArray(ConnectorConfigurationStatus[]::new);
    }

    /**
     * Returns whether this status is before the provided configuration status during the wizard
     * phase.
     *
     * @param status configuration status to check
     * @return whether this status is before the provided configuration status during the wizard
     *     phase
     */
    public boolean isBefore(ConnectorConfigurationStatus status) {
      return this.ordinal() < status.ordinal();
    }

    /**
     * Returns whether this status is after the provided configuration status during the wizard
     * phase.
     *
     * @param status configuration status to check
     * @return whether this status is after the provided configuration status during the wizard
     *     phase
     */
    public boolean isAfter(ConnectorConfigurationStatus status) {
      return this.ordinal() > status.ordinal();
    }
  }
}
