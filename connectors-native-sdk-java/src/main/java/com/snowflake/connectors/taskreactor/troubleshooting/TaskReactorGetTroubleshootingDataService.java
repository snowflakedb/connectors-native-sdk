/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.troubleshooting;

import com.snowflake.connectors.taskreactor.TaskReactorExistenceVerifier;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.registry.TaskReactorInstance;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** A service providing troubleshooting data about the Task Reactor. */
public class TaskReactorGetTroubleshootingDataService {

  private final Session session;
  private final TaskReactorExistenceVerifier taskReactorExistenceVerifier;
  private final InstanceRegistryRepository instanceRegistryRepository;

  /**
   * Returns Task Reactor troubleshooting data.
   *
   * @param session Snowpark session object
   * @param from data start timestamp
   * @param to data end timestamp
   * @return troubleshooting data
   */
  public static DataFrame getTroubleshootingData(Session session, long from, long to) {
    return new TaskReactorGetTroubleshootingDataService(
            session,
            TaskReactorExistenceVerifier.getInstance(session),
            InstanceRegistryRepository.getInstance(session))
        .getTroubleshootingData(from, to);
  }

  private TaskReactorGetTroubleshootingDataService(
      Session session,
      TaskReactorExistenceVerifier taskReactorExistenceVerifier,
      InstanceRegistryRepository instanceRegistryRepository) {
    this.session = session;
    this.taskReactorExistenceVerifier = taskReactorExistenceVerifier;
    this.instanceRegistryRepository = instanceRegistryRepository;
  }

  /**
   * Returns Task Reactor troubleshooting data.
   *
   * @param from data start timestamp
   * @param to data end timestamp
   * @return troubleshooting data
   */
  public DataFrame getTroubleshootingData(long from, long to) {
    if (!taskReactorExistenceVerifier.isTaskReactorConfigured()) {
      return emptyDataFrame();
    }

    List<TaskReactorInstance> instances = fetchTaskReactorInstances();
    DataFrame df = getInstancesData();
    return instances.stream()
        .map(instance -> getTroubleshootingDataForInstance(instance, from, to))
        .reduce(df, DataFrame::union);
  }

  private DataFrame emptyDataFrame() {
    return session.sql("select null, null where 1 = 0");
  }

  private DataFrame getInstancesData() {
    return session.sql(
        "select 'task_reactor_instance_registry' as category, TO_VARIANT(OBJECT_CONSTRUCT(*)) as"
            + " data FROM TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY");
  }

  private DataFrame getTroubleshootingDataForInstance(
      TaskReactorInstance instance, long fromTimestamp, long toTimestamp) {
    return new TaskReactorInstanceTroubleshootingDataService(session, instance.instanceName())
        .getTroubleshootingData(fromTimestamp, toTimestamp);
  }

  private List<TaskReactorInstance> fetchTaskReactorInstances() {
    return instanceRegistryRepository.fetchAll();
  }
}
