/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Comparator;

/** Fake, testing implementation of a {@link KeyValueTable} entry. */
public class FakeKeyValue {

  private Variant value;
  private final Instant timestamp;
  private Instant updatedAt;

  public FakeKeyValue(Variant value, Instant timestamp) {
    this.value = value;
    this.timestamp = timestamp;
    this.updatedAt = timestamp;
  }

  public Variant getValue() {
    return value;
  }

  public void setValue(Variant value) {
    this.value = value;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }

  public static class FakeKeyValueComparator implements Comparator<FakeKeyValue> {

    @Override
    public int compare(FakeKeyValue o1, FakeKeyValue o2) {
      return o2.timestamp.compareTo(o1.timestamp);
    }
  }
}
