/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import static java.lang.String.format;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link AccessTools}. */
class DefaultAccessTools implements AccessTools {

  private final Session session;

  DefaultAccessTools(Session session) {
    this.session = session;
  }

  @Override
  public boolean hasWarehouseAccess(Identifier warehouse) {
    var query = format("SHOW WAREHOUSES LIKE '%s'", warehouse.getName());
    return session.sql(query).collect().length == 1;
  }

  @Override
  public boolean hasSchemaAccess(Identifier schema) {
    var query = format("SHOW SCHEMAS LIKE '%s'", schema.getName());
    return session.sql(query).collect().length == 1;
  }
}
