/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Read-only implementation of {@link KeyValueTable}. Any use of update/delete operations will
 * trigger an {@link UnsupportedOperationException}.
 *
 * <p>This implementation requires that the backing Snowflake table has the following structure:
 *
 * <ul>
 *   <li>column {@code key} of type {@code STRING}
 *   <li>column {@code value} of type {@code Variant}
 * </ul>
 */
public class ReadOnlyKeyValueTable implements KeyValueTable {

  private final Session session;
  private final String tableName;

  /**
   * Creates a new {@link ReadOnlyKeyValueTable}, backed by the provided Snowflake table.
   *
   * @param session Snowpark session object
   * @param tableName name of the Snowflake table
   */
  public ReadOnlyKeyValueTable(Session session, String tableName) {
    this.tableName = tableName;
    this.session = session;
  }

  @Override
  public Variant fetch(String key) {
    Optional<Row> value =
        session.table(tableName).filter(col("KEY").equal_to(lit(key))).select(col("VALUE")).first();
    return value.map(row -> row.getVariant(0)).orElseThrow(() -> new KeyNotFoundException(key));
  }

  @Override
  public Map<String, Variant> fetchAll() {
    Row[] values = session.table(tableName).select("KEY", "VALUE").collect();
    return Arrays.stream(values)
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getVariant(1)));
  }

  @Override
  public void update(String key, Variant value) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Override
  public List<Variant> getAllWhere(Column filter) {
    Row[] values = session.table(tableName).filter(filter).select(col("VALUE")).collect();
    return Arrays.stream(values).map(row -> row.getVariant(0)).collect(Collectors.toList());
  }

  @Override
  public void updateMany(List<String> ids, String fieldName, Variant fieldValue) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Override
  public void updateAll(List<KeyValue> keyValues) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Override
  public void delete(String key) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }
}
