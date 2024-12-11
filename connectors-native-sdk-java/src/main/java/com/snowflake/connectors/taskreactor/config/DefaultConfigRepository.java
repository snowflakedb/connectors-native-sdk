/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.sysdate;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** Default implementation of {@link ConfigRepository} repository. */
class DefaultConfigRepository implements ConfigRepository {

  private final Session session;
  private final ObjectName configTableName;

  DefaultConfigRepository(Session session, ObjectName configTableName) {
    this.session = session;
    this.configTableName = configTableName;
  }

  @Override
  public TaskReactorConfig getConfig() {
    Row[] rows = session.table(configTableName.getValue()).select("key", "value").collect();
    Map<String, String> properties =
        Arrays.stream(rows)
            .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));

    return new TaskReactorConfig(
        properties.get("SCHEMA"),
        properties.get("WORKER_PROCEDURE"),
        properties.get("WORK_SELECTOR_TYPE"),
        properties.get("WORK_SELECTOR"),
        properties.get("EXPIRED_WORK_SELECTOR"),
        properties.get("WAREHOUSE"));
  }

  @Override
  public void update(String key, String value) {
    requireNonNull(key, "Key must not be null");
    requireNonNull(value, "Value must not be null");

    long updatedRows =
        session
            .table(configTableName.getValue())
            .update(
                Map.of(
                    col("value"), lit(value),
                    col("updated_at"), sysdate()),
                col("key").equal_to(lit(key)))
            .getRowsUpdated();

    if (updatedRows != 1) {
      throw new RuntimeException(
          format("Invalid number of updated rows: %d, expected 1", updatedRows));
    }
  }
}
