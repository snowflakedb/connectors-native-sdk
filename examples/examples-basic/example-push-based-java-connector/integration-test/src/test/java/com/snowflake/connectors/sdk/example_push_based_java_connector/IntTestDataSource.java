/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IntTestDataSource extends RandomDataSource {

  public List<Map<String, Object>> fetchDeltaRecordsWithIds(int... ids) {
    return Arrays.stream(ids).mapToObj(this::fetchRecord).collect(toList());
  }

  private Map<String, Object> fetchRecord(int id) {
    Map<String, Object> row = new HashMap<>();
    row.put("ID", id);
    row.put("COL_1", LocalDateTime.now().toString());
    row.put("COL_2", UUID.randomUUID().toString());
    row.put("TS", Instant.now());
    return row;
  }
}
