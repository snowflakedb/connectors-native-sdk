/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.taskreactor.utils.TaskReactorInstanceConfiguration;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import org.junit.jupiter.api.Test;

public class TestPlaceholder extends BaseTaskReactorIntegrationTest {

  /*
  This test is created only in purpose of showing how to set up and clean up the test with
  the usage of TaskReactorInstance class.
  */
  @Test
  void shouldPass() {
    var instance =
        TaskReactorTestInstance.buildFromScratch("TEST_INSTANCE", session)
            .withQueue()
            .withConfig(TaskReactorInstanceConfiguration.builder().build())
            .withWorkerStatus()
            .withWorkerRegistry()
            .withCommandsQueue()
            .createInstance();
    instance.delete();
    assert true;
  }
}
