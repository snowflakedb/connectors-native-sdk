/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import java.util.Objects;

/** Representation of worker id. */
public final class WorkerId {

  private final int value;

  /**
   * Creates a new {@link WorkerId}.
   *
   * @param value worker id value
   */
  public WorkerId(int value) {
    this.value = value;
  }

  /**
   * Returns the value of this worker id.
   *
   * @return value of this worker id
   */
  public int value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerId workerId = (WorkerId) o;
    return value == workerId.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return "WorkerId[id = " + value + "]";
  }
}
