/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.object_insert;
import static com.snowflake.snowpark_java.Functions.sysdate;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import com.snowflake.connectors.util.sql.MergeStatementValidator;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link KeyValueTable}.
 *
 * <p>This implementation requires that the backing Snowflake table has the following structure:
 *
 * <ul>
 *   <li>column {@code key} of type {@code STRING}
 *   <li>column {@code value} of type {@code Variant}
 *   <li>column {@code updated_at} of type {@code TIMESTAMP_NTZ}
 * </ul>
 */
public class DefaultKeyValueTable implements KeyValueTable {

  /** Maximum limit of inserted/updated records. */
  public static final int EXPRESSION_LIMIT = 16384;

  private static final StructType SCHEMA =
      StructType.create(
          new StructField("key", DataTypes.StringType),
          new StructField("value", DataTypes.VariantType),
          new StructField("updated_at", DataTypes.TimestampType));
  private final Session session;
  private final String tableName;

  /**
   * Creates a new {@link DefaultKeyValueTable}, backed by the provided Snowflake table.
   *
   * @param session Snowpark session object
   * @param tableName name of the Snowflake table
   */
  public DefaultKeyValueTable(Session session, String tableName) {
    this.tableName = tableName;
    this.session = session;
  }

  @Override
  public Variant fetch(String key) {
    return session
        .table(tableName)
        .filter(col("KEY").equal_to(lit(key)))
        .select("VALUE")
        .first()
        .map(row -> row.getVariant(0))
        .orElseThrow(() -> new KeyNotFoundException(key));
  }

  @Override
  public Map<String, Variant> fetchAll() {
    var result = session.table(tableName).select("KEY", "VALUE").collect();
    return Arrays.stream(result)
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getVariant(1)));
  }

  /**
   * {@inheritDoc}
   *
   * @throws RuntimeException when the number of affected rows is different from 1
   */
  @Override
  public void update(String key, Variant value) {
    requireNonNull(key, "Key must not be null");
    requireNonNull(value, "Value must not be null");

    var table = session.table(tableName);
    var source =
        session.createDataFrame(
            new Row[] {Row.create(key, value, Timestamp.from(Instant.now()))}, SCHEMA);

    var assignments =
        Map.of(
            col("KEY"), lit(key),
            col("VALUE"), lit(value.asJsonString()),
            col("UPDATED_AT"), sysdate());

    var mergeResult =
        table
            .merge(source, source.col("key").equal_to(table.col("key")))
            .whenMatched()
            .update(assignments)
            .whenNotMatched()
            .insert(assignments)
            .collect();

    if (mergeResult.getRowsInserted() + mergeResult.getRowsUpdated() != 1) {
      throw new RuntimeException(
          "Invalid number of changes in config table "
              + mergeResult.getRowsInserted()
              + " "
              + mergeResult.getRowsUpdated());
    }
  }

  @Override
  public List<Variant> getAllWhere(Column filter) {
    var values = session.table(tableName).filter(filter).select(col("VALUE")).collect();
    return Arrays.stream(values).map(row -> row.getVariant(0)).collect(Collectors.toList());
  }

  @Override
  public void updateMany(List<String> ids, String fieldName, Variant fieldValue) {
    var target = session.table(tableName);
    var source = session.table(tableName).filter(col("KEY").in(ids.toArray()));
    var assignments =
        Map.of(
            col("VALUE"),
            object_insert(col("VALUE"), lit(fieldName), lit(fieldValue.toString()), lit(true)),
            col("UPDATED_AT"),
            sysdate());

    target
        .merge(source, target.col("KEY").equal_to(source.col("KEY")))
        .whenMatched()
        .update(assignments)
        .collect();
  }

  @Override
  public void updateAll(List<KeyValue> keyValues) {
    if (keyValues.isEmpty()) {
      return;
    }

    MergeStatementValidator.validateRecordLimit(keyValues);
    MergeStatementValidator.validateDuplicates(keyValues, KeyValue::key);

    var rows = keyValues.stream().map(keyValue -> "(?,?)").collect(joining(","));
    Connection connection = session.jdbcConnection();
    var query =
        String.format(
            "MERGE INTO %s AS target USING (SELECT key, PARSE_JSON(value) AS VALUE FROM (VALUES%s)"
                + " AS V(key,value)) AS source ON target.key = source.key WHEN MATCHED THEN UPDATE"
                + " SET target.key = source.key, target.value = source.value, target.updated_at ="
                + " SYSDATE() WHEN NOT MATCHED THEN INSERT (\"KEY\", \"VALUE\", \"UPDATED_AT\")"
                + " VALUES (source.key, source.value, SYSDATE()) ",
            tableName, rows);

    try {
      PreparedStatement preparedStatement = connection.prepareStatement(query);
      int i = 1;
      for (KeyValue keyValue : keyValues) {
        preparedStatement.setString(i++, keyValue.key());
        preparedStatement.setString(i++, keyValue.value().asString());
      }

      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(String key) {
    session.table(tableName).delete(col("KEY").equal_to(lit(key)));
  }
}
