/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class RandomDataSource implements DataSource {

  private static final Random RANDOM = new Random();

  @Override
  public List<Map<String, Object>> fetchInitialRecords(int recordCount) {
    return IntStream.rangeClosed(1, recordCount)
        .mapToObj(this::fetchInitialRecord)
        .collect(toList());
  }

  private Map<String, Object> fetchInitialRecord(int id) {
    Map<String, Object> row = new HashMap<>();
    row.put("ID", id);
    row.put("COL_1", Instant.now().toString());
    row.put("COL_2", UUID.randomUUID().toString());
    row.put("OPERATION", "INSERT");
    return row;
  }

  @Override
  public List<Map<String, Object>> fetchRecords(int recordCount) {
    return IntStream.rangeClosed(1, recordCount).mapToObj(x -> fetchRecord()).collect(toList());
  }

  private Map<String, Object> fetchRecord() {
    Map<String, Object> row = new HashMap<>();
    row.put("ID", RANDOM.nextInt(500));
    row.put("COL_1", LocalDateTime.now().toString());
    row.put("COL_2", UUID.randomUUID().toString());
    row.put("TS", Instant.now());
    return row;
  }
}
