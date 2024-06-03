/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InMemoryWorkItemQueueTest {
  InMemoryWorkItemQueue queue;

  @BeforeEach
  void beforeEach() {
    queue = new InMemoryWorkItemQueue();
  }

  @Test
  void shouldPushItemsToQueue() {
    // given
    WorkItem workItem1 = new WorkItem("1", "2", new Variant("payload"));
    WorkItem workItem2 = new WorkItem("2", "2", new Variant("payload"));

    // when
    queue.push(List.of(workItem1, workItem2));

    // then
    assertThat(queue.store())
        .extracting(queueItem -> queueItem.id)
        .containsExactlyInAnyOrder(workItem1.id, workItem2.id);
  }

  @Test
  void shouldDeleteItemFromQueue() {
    // given
    WorkItem workItem1 = new WorkItem("1", "2", new Variant("payload"));
    WorkItem workItem2 = new WorkItem("2", "2", new Variant("payload"));
    queue.push(List.of(workItem1, workItem2));

    // when
    queue.delete(List.of("1"));

    // then
    assertThat(queue.store())
        .extracting(queueItem -> queueItem.id)
        .containsExactlyInAnyOrder(workItem2.id);
  }

  @Test
  void shouldCancelItemToQueue() {
    // when
    queue.cancelOngoingExecution("1");

    // then
    assertThat(queue.store())
        .extracting(
            queueItem -> queueItem.resourceId, queueItem -> queueItem.cancelOngoingExecution)
        .containsExactly(tuple("1", true));
  }

  @Test
  void shouldDeleteItemsBeforeGivenTimestampFromQueue() {
    // given
    WorkItem workItem1 = new WorkItem("1", "2", new Variant("payload"));
    WorkItem workItem2 = new WorkItem("2", "2", new Variant("payload"));
    queue.push(workItem1);

    // when
    Timestamp timestamp = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
    queue.deleteBefore("2", timestamp);
    queue.push(workItem2);

    // then
    assertThat(queue.store())
        .extracting(queueItem -> queueItem.id)
        .containsExactlyInAnyOrder(workItem2.id);
  }
}
