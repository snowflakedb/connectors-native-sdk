/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static java.lang.String.format;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link AccessTools}. */
class DefaultAccessTools implements AccessTools {

  private final Session session;

  DefaultAccessTools(Session session) {
    this.session = session;
  }

  @Override
  public boolean hasWarehouseAccess(Identifier warehouse) {
    var query = format("SHOW WAREHOUSES LIKE %s", asVarchar(warehouse.getUnquotedValue()));
    return resultExists(query);
  }

  @Override
  public boolean hasDatabaseAccess(Identifier database) {
    var query = format("SHOW DATABASES LIKE %s", asVarchar(database.getUnquotedValue()));
    return resultExists(query);
  }

  @Override
  public boolean hasSchemaAccess(SchemaName schemaName) {
    try {
      var query =
          format(
              "SHOW SCHEMAS LIKE %s IN DATABASE %s",
              asVarchar(schemaName.getSchema().getUnquotedValue()),
              schemaName.getDatabase().map(Identifier::getValue).orElseThrow());
      return resultExists(query);
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean hasSecretAccess(ObjectName secret) {
    try {
      var query = format("DESC SECRET %s", secret.getValue());
      return resultExists(query);
    } catch (Exception e) {
      return false;
    }
  }

  private boolean resultExists(String query) {
    return session.sql(query).collect().length == 1;
  }
}
