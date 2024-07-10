/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.task;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.snowpark_java.types.Variant;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link Command}. */
public class CommandAssert extends AbstractAssert<CommandAssert, Command> {

  public CommandAssert(Command command, Class<?> selfType) {
    super(command, selfType);
  }

  /**
   * Asserts that this command have an id equal to the specified value.
   *
   * @param id expected id
   * @return this assertion
   */
  public CommandAssert hasId(String id) {
    assertThat(actual.getId()).isEqualTo(id);
    return this;
  }

  /**
   * Asserts that this command have a payload equal to the specified value.
   *
   * @param payload expected payload
   * @return this assertion
   */
  public CommandAssert hasPayload(Variant payload) {
    assertThat(actual.getPayload()).isEqualTo(payload);
    return this;
  }

  /**
   * Asserts that this command have an seqNo equal to the specified value.
   *
   * @param seqNo expected seqNo
   * @return this assertion
   */
  public CommandAssert hasSeqNo(long seqNo) {
    assertThat(actual.getSeqNo()).isEqualTo(seqNo);
    return this;
  }

  /**
   * Asserts that this command have a commandType equal to the specified value.
   *
   * @param commandType expected commandType
   * @return this assertion
   */
  public CommandAssert hasCommandType(Command.CommandType commandType) {
    assertThat(actual.getType()).isEqualTo(commandType);
    return this;
  }
}
