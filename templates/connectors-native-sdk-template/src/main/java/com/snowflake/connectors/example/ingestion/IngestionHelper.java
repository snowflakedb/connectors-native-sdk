/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import static com.snowflake.snowpark_java.Functions.col;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;

/** A utility class for resource ingestion handling. */
public final class IngestionHelper {

  private IngestionHelper() {}

  /**
   * Save the raw data to a database table
   *
   * @param session Snowpark session object
   * @param destTableName Target table name
   * @param data Raw data to save
   * @return number of rows saved in the destination table
   */
  public static long saveRawData(Session session, String destTableName, List<Variant> data) {
    // TODO: This method is responsible for inserting/updating data in the sink table. If the data
    // is stored in different format than raw variants the it needs to be customized

    var tableSchema = StructType.create(new StructField("RAW_DATA", DataTypes.VariantType));
    var dataRows = data.stream().map(Row::create).toArray(Row[]::new);
    var source = session.createDataFrame(dataRows, tableSchema);
    var table = session.table(destTableName);
    var assignments = Map.of(col("RAW_DATA"), source.col("RAW_DATA"));
    table
        .merge(
            source,
            table.col("raw_data").subField("id").equal_to(source.col("raw_data").subField("id")))
        .whenMatched()
        .update(assignments)
        .whenNotMatched()
        .insert(assignments)
        .collect();
    return dataRows.length;
  }
}
