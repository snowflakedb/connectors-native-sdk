/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.snowpark_java.Session;

/** A utility for verifying whether the Task Reactor is enabled. */
public interface TaskReactorExistenceVerifier {

  /**
   * The method that check if the Task Reactor is used by the application.
   *
   * @return boolean that indicates whether the Task Reactor is used by the application or not.
   */
  boolean isTaskReactorConfigured();

  /**
   * Gets a new instance of the default task reactor existence verifier.
   *
   * @param session Snowpark session object
   * @return a new default task reactor existence verifier instance
   */
  static TaskReactorExistenceVerifier getInstance(Session session) {
    return new DefaultTaskReactorExistenceVerifier(session);
  }
}
