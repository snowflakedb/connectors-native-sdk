/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import java.util.Objects;

/**
 * Placeholder class for use as a {@link ResourceIngestionDefinition} parameter, when there is no
 * need to define custom properties for a given definition implementation.
 */
public class EmptyImplementation {

  /** Creates a new {@link EmptyImplementation}. */
  public EmptyImplementation() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    return o != null && getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
