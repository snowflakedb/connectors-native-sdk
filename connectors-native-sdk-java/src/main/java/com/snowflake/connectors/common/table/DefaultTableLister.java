/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.common.object.Identifier.from;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.quoted;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.SchemaName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** {@inheritDoc} This implementation uses SHOW TABLES ... IN SCHEMA statement */
public class DefaultTableLister implements TableLister {

  private final Session session;

  /**
   * Creates a new {@link DefaultTableLister}.
   *
   * @param session Snowpark session object
   */
  public DefaultTableLister(Session session) {
    this.session = session;
  }

  /** {@inheritDoc} */
  @Override
  public List<TableProperties> showTables(SchemaName schema) {
    return showTablesInternal(schema, null);
  }

  /** {@inheritDoc} */
  @Override
  public List<TableProperties> showTables(SchemaName schema, String like) {
    return showTablesInternal(schema, like);
  }

  List<TableProperties> showTablesInternal(SchemaName schema, String likePattern) {

    requireNonNull(schema, "schema cannot be null");
    var query = makeQuery(schema, likePattern);

    return Arrays.stream(
            session
                .sql(query)
                .select(
                    quoted("created_on"),
                    quoted("name"),
                    quoted("database_name"),
                    quoted("schema_name"),
                    quoted("kind"),
                    quoted("owner"),
                    quoted("rows"))
                .collect())
        .map(this::mapToTableProperties)
        .collect(Collectors.toList());
  }

  private TableProperties mapToTableProperties(Row row) {
    var createdOn = row.getTimestamp(0);
    var tableName = row.getString(1);
    var databaseName = row.getString(2);
    var schemaName = row.getString(3);
    var kind = row.getString(4);
    var owner = row.getString(5);
    var rows = row.getLong(6);
    return new TableProperties(
        from(databaseName, Identifier.AutoQuoting.ENABLED),
        from(schemaName, Identifier.AutoQuoting.ENABLED),
        from(tableName, Identifier.AutoQuoting.ENABLED),
        createdOn,
        kind,
        owner,
        rows);
  }

  private static String makeQuery(SchemaName schema, String likePattern) {
    var like = Optional.ofNullable(likePattern);
    StringBuilder query = new StringBuilder("show tables");
    like.ifPresent(pattern -> query.append(" like ").append(asVarchar(pattern)));
    query.append(" in schema ");
    query.append(schema.getValue());
    return query.toString();
  }
}
