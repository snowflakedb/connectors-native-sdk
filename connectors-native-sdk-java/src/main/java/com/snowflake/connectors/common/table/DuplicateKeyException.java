/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.exception.ConnectorException;
import java.util.Set;
import java.util.stream.Collectors;

/** Exception thrown when there are duplicated keys in collection */
public class DuplicateKeyException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "DUPLICATED_KEYS";

  private static final String MESSAGE =
      "There were duplicated keys in the collection. Duplicated IDs found [%s].";

  /**
   * Creates a new {@link DuplicateKeyException}.
   *
   * @param duplicatedKeys record limit
   */
  public DuplicateKeyException(Set<?> duplicatedKeys) {
    super(
        RESPONSE_CODE,
        String.format(
            MESSAGE,
            duplicatedKeys.stream()
                .map(Object::toString)
                .sorted()
                .collect(Collectors.joining(", "))));
  }
}
