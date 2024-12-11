/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.common.table.CascadeMode.CASCADE;
import static com.snowflake.connectors.common.table.CascadeMode.RESTRICT;
import static com.snowflake.connectors.common.table.DropMode.IF_EXISTS;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Implements operations on snowflake TABLE objects */
public class DefaultTableRepository implements TableRepository {

  private final Session session;
  private final TableLister tableLister;

  /**
   * Creates a new {@link DefaultTableRepository}, using a default {@link TableLister}
   * implementation.
   *
   * @param session Snowpark session object
   */
  public DefaultTableRepository(Session session) {
    this.session = session;
    tableLister = new DefaultTableLister(session);
  }

  @Override
  public void dropTableIfExists(ObjectName table) {
    dropTableInternal(table, IF_EXISTS, RESTRICT);
  }

  @Override
  public void renameTable(ObjectName oldTable, ObjectName newTable) {
    renameTableInternal(oldTable, newTable);
  }

  @Override
  public List<TableProperties> showTables(SchemaName schema) {
    return tableLister.showTables(schema);
  }

  @Override
  public List<TableProperties> showTables(SchemaName schema, String like) {
    return tableLister.showTables(schema, like);
  }

  private void dropTableInternal(ObjectName table, DropMode dropMode, CascadeMode cascadeMode) {
    var drop = new StringBuilder("drop table ");
    if (IF_EXISTS.equals(dropMode)) {
      drop.append(" if exists ");
    }
    drop.append(table.getValue());
    if (CASCADE.equals(cascadeMode)) {
      drop.append(" cascade ");
    } else {
      drop.append(" restrict ");
    }
    session.sql(drop.toString()).collect();
  }

  private void renameTableInternal(ObjectName oldTable, ObjectName newTable) {
    var rename =
        String.format("alter table %s rename to %s", oldTable.getValue(), newTable.getValue());
    session.sql(rename.toString()).collect();
  }
}
