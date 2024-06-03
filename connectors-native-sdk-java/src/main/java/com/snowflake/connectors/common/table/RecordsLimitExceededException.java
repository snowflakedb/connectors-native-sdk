/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when too many records are inserted/updated. */
public class RecordsLimitExceededException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "RECORDS_LIMIT_EXCEEDED";

  private static final String MESSAGE =
      "Limit of singular update for many properties is: %d. "
          + "Reduce amount of passed keyValues or split them into chunks smaller or equal to: %d";

  /**
   * Creates a new {@link RecordsLimitExceededException}.
   *
   * @param limit record limit
   */
  public RecordsLimitExceededException(int limit) {
    super(RESPONSE_CODE, String.format(MESSAGE, limit, limit));
  }
}
