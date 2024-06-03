/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import com.snowflake.connectors.common.table.DuplicateKeyException;
import com.snowflake.connectors.common.table.RecordsLimitExceededException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/** Validator for merge statements executed in Snowflake. */
public class MergeStatementValidator {

  /** Maximum limit of inserted/updated records. */
  public static final int EXPRESSION_LIMIT = 16_384;

  private MergeStatementValidator() {}

  /**
   * Validates if collection size is exceeding statement limit of 16384.
   *
   * @param collection Validated collection
   * @param <T> Type of the collection
   */
  public static <T> void validateRecordLimit(Collection<T> collection) {
    if (collection.size() > EXPRESSION_LIMIT) {
      throw new RecordsLimitExceededException(EXPRESSION_LIMIT);
    }
  }

  /**
   * Validates if collection contains only unique keys.
   *
   * @param collection validated collection
   * @param idExtractor function for extraction of each item identifier
   * @param <T> type of item
   * @param <K> type of item identifier
   */
  public static <T, K> void validateDuplicates(
      Collection<T> collection, Function<T, K> idExtractor) {
    Set<K> uniqueKeys = new HashSet<>();
    Set<K> duplicates = new HashSet<>();
    collection.stream()
        .map(idExtractor)
        .forEach(
            key -> {
              if (!uniqueKeys.add(key)) {
                duplicates.add(key);
              }
            });

    if (duplicates.size() > 0) {
      throw new DuplicateKeyException(duplicates);
    }
  }
}
