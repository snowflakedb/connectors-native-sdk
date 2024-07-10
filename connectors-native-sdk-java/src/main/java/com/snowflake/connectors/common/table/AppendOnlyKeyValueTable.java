/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.row_number;
import static com.snowflake.snowpark_java.Window.partitionBy;
import static java.util.Objects.requireNonNull;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.WindowSpec;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link AppendOnlyTable}, which stores the data as key-value pairs,
 * similarly to the default implementation of {@link KeyValueTable}.
 *
 * <p>This implementation requires that the backing Snowflake table has the following structure:
 *
 * <ul>
 *   <li>column {@code key} of type {@code STRING}
 *   <li>column {@code value} of type {@code Variant}
 *   <li>column {@code updated_at} of type {@code TIMESTAMP_NTZ}
 * </ul>
 */
public class AppendOnlyKeyValueTable implements AppendOnlyTable {

  private static final StructType SCHEMA =
      StructType.create(
          new StructField("timestamp", DataTypes.TimestampType),
          new StructField("key", DataTypes.StringType),
          new StructField("value", DataTypes.VariantType));
  private static final WindowSpec PARTITION_BY_TIMESTAMP =
      partitionBy(col("KEY")).orderBy(col("TIMESTAMP").desc());
  private static final Column ROW_NUMBER_COL = row_number().over(PARTITION_BY_TIMESTAMP).as("ROW");

  private final Session session;
  private final String tableName;

  /**
   * Creates a new {@link AppendOnlyKeyValueTable}, backed by the provided Snowflake table.
   *
   * @param session Snowpark session object
   * @param tableName name of the Snowflake table
   */
  public AppendOnlyKeyValueTable(Session session, String tableName) {
    this.tableName = tableName;
    this.session = session;
  }

  @Override
  public Variant fetch(String key) {
    return session
        .table(tableName)
        .filter(col("KEY").equal_to(lit(key)))
        .sort(col("TIMESTAMP").desc())
        .select("VALUE")
        .first()
        .map(row -> row.getVariant(0))
        .orElseThrow(() -> new KeyNotFoundException(key));
  }

  @Override
  public void insert(String key, Variant value) {
    requireNonNull(key, "Key must not be null");
    requireNonNull(value, "Value must not be null");
    var timestamp = Timestamp.from(Instant.now());
    var row = Row.create(timestamp, key, value);
    session.createDataFrame(new Row[] {row}, SCHEMA).write().mode("append").saveAsTable(tableName);
  }

  @Override
  public List<Variant> getAllWhere(Column filter) {
    var values =
        session
            .table(tableName)
            .filter(filter)
            .select(col("VALUE"), ROW_NUMBER_COL)
            .filter(col("ROW").equal_to(lit(1)))
            .collect();

    return Arrays.stream(values).map(row -> row.getVariant(0)).collect(Collectors.toList());
  }
}
