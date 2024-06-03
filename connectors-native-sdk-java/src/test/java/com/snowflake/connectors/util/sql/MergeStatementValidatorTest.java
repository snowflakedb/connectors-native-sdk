/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.snowflake.connectors.common.table.DuplicateKeyException;
import com.snowflake.connectors.common.table.RecordsLimitExceededException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class MergeStatementValidatorTest {
  @Test
  void shouldNotThrowWhenCollectionLimitWasNotExceeded() {
    // given
    List<String> list = List.of("one");

    // then
    assertThatNoException().isThrownBy(() -> MergeStatementValidator.validateRecordLimit(list));
  }

  @Test
  void shouldThrowWhenCollectionLimitWasExceeded() {
    // given
    List<String> list =
        IntStream.range(0, 100_000)
            .mapToObj(i -> String.format("item-%d", i))
            .collect(Collectors.toList());

    // then
    assertThatExceptionOfType(RecordsLimitExceededException.class)
        .isThrownBy(() -> MergeStatementValidator.validateRecordLimit(list));
  }

  @Test
  void shouldNotThrowWhenThereAreNoDuplicates() {
    // given
    List<UUID> list = List.of(UUID.randomUUID(), UUID.randomUUID());

    // then
    assertThatNoException()
        .isThrownBy(() -> MergeStatementValidator.validateDuplicates(list, value -> value));
  }

  @Test
  void shouldThrowWhenThereAreDuplicates() {
    // given
    UUID duplicateUUID = UUID.fromString("2b31974a-9663-4cf7-b11e-7285c22658e0");
    UUID secondDuplicateUUID = UUID.fromString("c95f0138-8cea-493d-8cd3-26584b8e370c");
    List<UUID> list =
        List.of(
            duplicateUUID,
            UUID.randomUUID(),
            duplicateUUID,
            secondDuplicateUUID,
            secondDuplicateUUID);

    // then
    assertThatExceptionOfType(DuplicateKeyException.class)
        .isThrownBy(() -> MergeStatementValidator.validateDuplicates(list, value -> value))
        .withMessage(
            String.format(
                "There were duplicated keys in the collection. Duplicated IDs found [%s, %s].",
                duplicateUUID, secondDuplicateUUID));
  }
}
