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

  /**
   * Creates a new {@link FakeKeyValue}.
   *
   * @param value entry value
   * @param timestamp entry timestamp
   */
  public FakeKeyValue(Variant value, Instant timestamp) {
    this.value = value;
    this.timestamp = timestamp;
    this.updatedAt = timestamp;
  }

  /**
   * Returns the entry value.
   *
   * @return the entry value
   */
  public Variant getValue() {
    return value;
  }

  /**
   * Sets the entry value.
   *
   * @param value new entry value
   */
  public void setValue(Variant value) {
    this.value = value;
  }

  /**
   * Returns the entry timestamp.
   *
   * @return the entry timestamp
   */
  public Instant getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the entry updated at timestamp.
   *
   * @return the entry updated at timestamp
   */
  public Instant getUpdatedAt() {
    return updatedAt;
  }

  /**
   * Sets the entry updated at timestamp.
   *
   * @param updatedAt the new entry updated at timestamp
   */
  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }

  /** A simple timestamp comparator for {@link FakeKeyValue} objects. */
  public static class FakeKeyValueComparator implements Comparator<FakeKeyValue> {

    @Override
    public int compare(FakeKeyValue o1, FakeKeyValue o2) {
      return o2.timestamp.compareTo(o1.timestamp);
    }
  }
}
