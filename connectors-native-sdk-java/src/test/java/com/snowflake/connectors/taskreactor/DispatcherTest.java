/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.AVAILABLE;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.SCHEDULED_FOR_CANCELLATION;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.WORK_ASSIGNED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.commands.processor.CommandsProcessor;
import com.snowflake.connectors.taskreactor.queue.InMemoryWorkItemQueue;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.queue.cleaner.QueueItemCleaner;
import com.snowflake.connectors.taskreactor.queue.selector.QueueItemSelector;
import com.snowflake.connectors.taskreactor.worker.InMemoryWorkerCombinedView;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerManager;
import com.snowflake.connectors.taskreactor.worker.queue.InMemoryWorkerQueue;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DispatcherTest {

  private final Identifier instanceSchema = Identifier.from("SCHEMA");
  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final WorkerManager workerManager = mock();
  private final InMemoryWorkerCombinedView workerCombinedView = new InMemoryWorkerCombinedView();
  private final InMemoryWorkItemQueue workItemQueue = new InMemoryWorkItemQueue();
  private final InMemoryWorkerQueue workerQueue = new InMemoryWorkerQueue();
  private final TaskRepository taskRepository = mock();
  private final TaskRef taskRef = mock();
  private final QueueItemSelector queueItemSelector = mock();
  private final CommandsProcessor commandsProcessor = mock();
  private final QueueItemCleaner queueItemCleaner = mock();
  private final InstanceStreamService instanceStreamService = mock();
  private final Dispatcher dispatcher =
      new Dispatcher(
          instanceSchema,
          workerStatusRepository,
          workerManager,
          workerCombinedView,
          workItemQueue,
          workerQueue,
          taskRepository,
          commandsProcessor,
          queueItemSelector,
          queueItemCleaner,
          instanceStreamService);

  @AfterEach
  void afterEach() {
    workerStatusRepository.clear();
    workItemQueue.clear();
    workerQueue.clear();
    Mockito.reset(taskRepository);
    Mockito.reset(taskRef);
    Mockito.reset(taskRef);
    Mockito.reset(commandsProcessor);
    Mockito.reset(queueItemSelector);
    Mockito.reset(queueItemCleaner);
  }

  @Test
  void shouldSkipExecutionWhenNoItemsWereFoundInQueue() {
    // given
    when(queueItemSelector.getSelectedItemsFromQueue()).thenReturn(Collections.emptyList());

    // when
    String result = dispatcher.execute();

    // then
    assertThat(result).isEqualTo("Work Selector selected no items for processing. Exiting.");
  }

  @Test
  void shouldDispatchItems() {
    // given
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    WorkItem workItem1 = WorkItem.from(queueItem1);
    WorkItem workItem2 = WorkItem.from(queueItem2);
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    WorkerId workerId3 = new WorkerId(3);
    workerStatusRepository.updateStatusesFor(AVAILABLE, List.of(workerId1, workerId2, workerId3));
    workItemQueue.push(List.of(workItem1, workItem2));

    when(queueItemSelector.getSelectedItemsFromQueue()).thenReturn(List.of(queueItem1, queueItem2));
    when(taskRepository.fetch(any())).thenReturn(taskRef);

    // when
    String result = dispatcher.execute();

    // then
    assertThat(result).isEqualTo("Consumed 2 items from the queue.");
    assertThat(workerQueue.store())
        .containsExactly(entry(workerId1, workItem1), entry(workerId2, workItem2));

    assertThat(workerStatusRepository.store())
        .containsExactly(
            entry(workerId1, WORK_ASSIGNED),
            entry(workerId2, WORK_ASSIGNED),
            entry(workerId3, AVAILABLE));
    assertThat(workItemQueue.store()).isEmpty();
    verify(taskRef, times(2)).resume();
  }

  @Test
  void shouldDispatchItemsUpToWorkerLimit() {
    // given
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    WorkItem workItem1 = WorkItem.from(queueItem1);
    WorkItem workItem2 = WorkItem.from(queueItem2);
    WorkerId workerId1 = new WorkerId(1);
    workerStatusRepository.updateStatusesFor(AVAILABLE, List.of(workerId1));
    workItemQueue.push(List.of(workItem1, workItem2));

    when(queueItemSelector.getSelectedItemsFromQueue()).thenReturn(List.of(queueItem1, queueItem2));
    when(taskRepository.fetch(any())).thenReturn(taskRef);

    // when
    String result = dispatcher.execute();

    // then
    assertThat(result).isEqualTo("Consumed 1 items from the queue.");
    assertThat(workerQueue.store()).containsExactly(entry(workerId1, workItem1));
    assertThat(workerStatusRepository.store()).containsExactly(entry(workerId1, WORK_ASSIGNED));
    assertThat(workItemQueue.store())
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload)
        .containsExactlyInAnyOrder(
            tuple(queueItem2.id, queueItem2.resourceId, queueItem2.workerPayload));
    verify(taskRef).resume();
  }

  @Test
  void shouldCancelQueuedItems() {
    // given
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    QueueItem queueItem3 = generate("3", "2");
    WorkItem workItem1 = WorkItem.from(queueItem1);
    WorkItem workItem2 = WorkItem.from(queueItem2);
    WorkItem workItem3 = WorkItem.from(queueItem3);
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    WorkerId workerId3 = new WorkerId(3);

    workerStatusRepository.updateStatusesFor(AVAILABLE, List.of(workerId1, workerId2, workerId3));
    workerCombinedView.addExecutingWorkers(List.of(workerId1));
    workItemQueue.push(List.of(workItem1, workItem2));

    when(taskRepository.fetch(any())).thenReturn(taskRef);

    // push canceling item
    workItemQueue.cancelOngoingExecution("2");
    workItemQueue.push(workItem3);

    when(queueItemSelector.getSelectedItemsFromQueue()).thenReturn(List.of(queueItem1, queueItem3));

    // when
    String result = dispatcher.execute();

    // then
    assertThat(result).isEqualTo("Consumed 2 items from the queue.");
    assertThat(workerQueue.store().entrySet())
        .extracting(
            Map.Entry::getKey,
            queueEntry -> queueEntry.getValue().id,
            queueEntry -> queueEntry.getValue().resourceId)
        .containsExactly(
            tuple(workerId2, workItem1.id, workItem1.resourceId),
            tuple(workerId3, workItem3.id, workItem3.resourceId));
    assertThat(workerStatusRepository.store())
        .containsExactly(
            entry(workerId1, SCHEDULED_FOR_CANCELLATION),
            entry(workerId2, WORK_ASSIGNED),
            entry(workerId3, WORK_ASSIGNED));
    assertThat(workItemQueue.store()).isEmpty();
    verify(taskRef, times(2)).resume();
  }

  @Test
  void shouldRecreateInstanceStreamsIfRequired() {
    // when
    dispatcher.execute();

    // then
    verify(instanceStreamService, times(1)).recreateStreamsIfRequired(instanceSchema);
  }

  private static QueueItem generate(String id, String resourceId) {
    return QueueItem.fromMap(
        Map.of(
            "id", new Variant(id),
            "timestamp", new Variant(Timestamp.from(Instant.now())),
            "resourceId", new Variant(resourceId),
            "workerPayload", new Variant("payload")));
  }
}
