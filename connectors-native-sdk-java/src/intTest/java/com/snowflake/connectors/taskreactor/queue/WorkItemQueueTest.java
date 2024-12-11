/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import static com.snowflake.connectors.common.IdGenerator.randomId;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WorkItemQueueTest extends BaseTaskReactorIntegrationTest {

  private static final String INSTANCE_NAME = "TEST_INSTANCE";

  private TaskReactorTestInstance instance;
  private WorkItemQueue workItemQueue;

  @BeforeEach
  void setUp() {
    instance =
        TaskReactorTestInstance.buildFromScratch(INSTANCE_NAME, session)
            .withQueue()
            .createInstance();
    workItemQueue = WorkItemQueue.getInstance(session, Identifier.from(INSTANCE_NAME));
  }

  @AfterEach
  void cleanUp() {
    instance.delete();
  }

  @Test
  void shouldInsertWorkItemToQueue() {
    // given
    WorkItem item1 = new WorkItem(randomId(), randomId(), new Variant(new HashMap<>()));
    WorkItem item2 = new WorkItem(randomId(), randomId(), new Variant(new HashMap<>()));
    WorkItem item3 = new WorkItem(randomId(), randomId(), new Variant(new HashMap<>()));

    // when
    workItemQueue.push(List.of(item1, item2, item3));

    // then
    List<WorkItem> actual = fetchAllWorkItemsFromQueue();
    assertThat(actual).containsExactlyInAnyOrder(item1, item2, item3);
  }

  private List<WorkItem> fetchAllWorkItemsFromQueue() {
    return Arrays.stream(
            session
                .sql("SELECT ID, RESOURCE_ID, WORKER_PAYLOAD FROM TEST_INSTANCE.QUEUE")
                .collect())
        .map(x -> new WorkItem(x.getString(0), x.getString(1), x.getVariant(2)))
        .collect(Collectors.toList());
  }
}
