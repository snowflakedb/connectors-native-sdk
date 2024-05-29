/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_github_connector.common;

import static com.snowflake.snowpark_java.Functions.col;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.SaveMode;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Map;

public class KeyValueTable {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String TIMESTAMP = "timestamp";

  private final Session session;
  private final String name;
  private final boolean appendOnly;

  public KeyValueTable(Session session, String name, boolean appendOnly) {
    this.session = session;
    this.name = name;
    this.appendOnly = appendOnly;
  }

  public Variant getValue(String key) {
    Row[] rows;
    if (this.appendOnly) {
      rows =
          session
              .table(name)
              .filter(col(KEY).equal_to(Functions.lit(key)))
              .sort(col(TIMESTAMP).desc())
              .limit(1)
              .select(col(VALUE))
              .collect();
    } else {
      rows =
          session
              .table(name)
              .filter(col(KEY).equal_to(Functions.lit(key)))
              .select(col(VALUE))
              .collect();
    }
    if (rows.length == 0) {
      return null;
    } else {
      return rows[0].getVariant(0);
    }
  }

  public void merge(String key, Variant value) {
    if (appendOnly) {
      var source = keyValueDataFrameWithTimestamp(key, value);
      source.write().mode(SaveMode.Append).saveAsTable(name);
    } else {
      var target = session.table(name);
      var source = keyValueDataFrame(key, value);
      target
          .merge(source, target.col(KEY).equal_to(source.col(KEY)))
          .whenMatched()
          .updateColumn(Map.of(KEY, source.col(VALUE)))
          .whenNotMatched()
          .insert(new Column[] {source.col(KEY), source.col(VALUE)})
          .collect();
    }
  }

  private DataFrame keyValueDataFrame(String key, Variant value) {
    var schema =
        StructType.create(
            new StructField(KEY, DataTypes.StringType),
            new StructField(VALUE, DataTypes.VariantType));
    return session.createDataFrame(new Row[] {Row.create(key, value)}, schema);
  }

  private DataFrame keyValueDataFrameWithTimestamp(String key, Variant value) {
    var schema =
        StructType.create(
            new StructField(TIMESTAMP, DataTypes.TimestampType),
            new StructField(KEY, DataTypes.StringType),
            new StructField(VALUE, DataTypes.VariantType));
    return session.createDataFrame(
        new Row[] {Row.create(new java.sql.Timestamp(Instant.now().toEpochMilli()), key, value)},
        schema);
  }
}
