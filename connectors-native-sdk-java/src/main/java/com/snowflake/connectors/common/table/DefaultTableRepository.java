/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Implements operations on snowflake TABLE objects */
public class DefaultTableRepository implements TableRepository {
  final Session session;
  final TableLister tableLister;

  /**
   * Create DefaultTableRepository
   *
   * @param session
   */
  public DefaultTableRepository(Session session) {
    this.session = session;
    tableLister = new DefaultTableLister(session);
  }

  /**
   * {@inheritDoc}
   *
   * @param table table to drop
   */
  @Override
  public void dropTableIfExists(ObjectName table) {
    dropTableInternal(table, true, false);
  }

  private void dropTableInternal(ObjectName table, boolean ifExists, boolean cascade) {
    var drop = new StringBuilder("drop table ");
    if (ifExists) {
      drop.append(" if exists ");
    }
    drop.append(table.getValue());
    if (cascade) {
      drop.append(" cascade ");
    }
    session.sql(drop.toString()).collect();
  }

  /** {@inheritDoc} */
  @Override
  public List<TableProperties> showTables(SchemaName schema) {
    return tableLister.showTables(schema);
  }

  /** {@inheritDoc} */
  @Override
  public List<TableProperties> showTables(SchemaName schema, String like) {
    return tableLister.showTables(schema, like);
  }
}
