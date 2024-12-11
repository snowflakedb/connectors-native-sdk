/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.prerequisites;

import com.snowflake.snowpark_java.Session;

/** A repository for basic management of prerequisites. */
public interface PrerequisitesRepository {

  /** Updates the prerequisites setting all as not completed. */
  void markAllPrerequisitesAsUndone();

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses the {@code STATE.PREREQUISITES} table.
   *
   * @param session Snowpark session object
   * @return a new repository instance
   */
  static PrerequisitesRepository getInstance(Session session) {
    return new DefaultPrerequisitesRepository(session);
  }
}
