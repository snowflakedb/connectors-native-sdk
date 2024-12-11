/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.prerequisites;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;

import com.snowflake.snowpark_java.Session;
import java.util.Map;

/** Default implementation of {@link PrerequisitesRepository}. */
class DefaultPrerequisitesRepository implements PrerequisitesRepository {

  private static final String TABLE_NAME = "STATE.PREREQUISITES";
  private final Session session;

  public DefaultPrerequisitesRepository(Session session) {
    this.session = session;
  }

  @Override
  public void markAllPrerequisitesAsUndone() {
    var table = session.table(TABLE_NAME);
    table.updateColumn(Map.of("IS_COMPLETED", lit(false)));
  }
}
