/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.snowpark_java.Session;

/** A set of basic Snowflake access tools. */
public interface AccessTools {

  /**
   * Returns whether the application has access to the specified warehouse.
   *
   * <p>It is assumed, that if the {@code SHOW WAREHOUSES} query returns the specified warehouse -
   * the application has access to it.
   *
   * @param warehouse warehouse identifier
   * @return whether the application has access to the specified warehouse
   */
  boolean hasWarehouseAccess(Identifier warehouse);

  /**
   * Returns whether the application has access to the specified database.
   *
   * <p>It is assumed, that if the {@code SHOW DATABASES} query returns the specified database - the
   * application has access to it.
   *
   * @param database database identifier
   * @return whether the application has access to the specified schema
   */
  boolean hasDatabaseAccess(Identifier database);

  /**
   * Returns whether the application has access to the specified schema in the specified database.
   *
   * <p>It is assumed, that if the {@code SHOW SCHEMAS} query returns the specified schema - the
   * application has access to it.
   *
   * @param schemaName schema name identifier
   * @return whether the application has access to the specified schema in the specified database
   */
  boolean hasSchemaAccess(SchemaName schemaName);

  /**
   * Returns whether the application has access to the specified secret.
   *
   * <p>It is assumed, that if the {@code DESC SECRET} query describes the specified secret - the
   * application has access to it.
   *
   * @param secret secret object name
   * @return whether the application has access to the specified secret
   */
  boolean hasSecretAccess(ObjectName secret);

  /**
   * Returns a new instance of the default tools implementation.
   *
   * @param session Snowpark session object
   * @return a new tools instance
   */
  static AccessTools getInstance(Session session) {
    return new DefaultAccessTools(session);
  }
}
