/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.connectors.taskreactor.commands.queue.InvalidCommandTypeException;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CommandTest {

  public static final String ID = "fixed id";
  public static final Variant EXAMPLE_PAYLOAD = new Variant("123");
  public static final Long EXAMPLE_SEQ_NO = 1L;

  @ParameterizedTest
  @MethodSource("provideValidCommandTypes")
  void shouldCreateCommandWhenCommandTypeIsValid(String commandType) {
    // expect
    assertThatNoException()
        .isThrownBy(() -> new Command(ID, commandType, EXAMPLE_PAYLOAD, EXAMPLE_SEQ_NO));
  }

  @Test
  void shouldThrowUnsupportedCommandTypeExceptionWhenCommandTypeIsInvalid() {
    // given
    String invalidCommandType = "invalidType";

    // expect
    assertThatExceptionOfType(InvalidCommandTypeException.class)
        .isThrownBy(() -> new Command(ID, invalidCommandType, EXAMPLE_PAYLOAD, EXAMPLE_SEQ_NO))
        .withMessage(
            format(
                "Command type [%s] is not recognized for Command with id [%s].",
                invalidCommandType, ID));
  }

  @ParameterizedTest
  @MethodSource("provideValidCommandTypes")
  void shouldReturnTrueWhenTypeIsValid(String commandType) {
    // expect
    assertThat(CommandType.isValidType(commandType)).isTrue();
  }

  @Test
  void shouldReturnFalseWhenTypeIsInvalid() {
    // given
    String invalidCommandType = "invalidType";

    // expect
    assertThat(CommandType.isValidType(invalidCommandType)).isFalse();
  }

  private static Stream<Arguments> provideValidCommandTypes() {
    return Arrays.stream(CommandType.values()).map(CommandType::name).map(Arguments::of);
  }
}
