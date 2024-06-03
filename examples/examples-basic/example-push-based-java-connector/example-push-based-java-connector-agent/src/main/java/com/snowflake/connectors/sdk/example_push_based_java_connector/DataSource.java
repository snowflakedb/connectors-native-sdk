/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import java.util.List;
import java.util.Map;

public interface DataSource {

  List<Map<String, Object>> fetchInitialRecords(int recordCount);

  List<Map<String, Object>> fetchRecords(int recordCount);
}
