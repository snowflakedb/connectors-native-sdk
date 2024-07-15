/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.log;

import com.snowflake.connectors.util.log.PrefixLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which should be used in order to create a logger for taskreactor module The logger adds a
 * prefix ConnectorsSDK::TaskReactor:: to all logged messages
 */
public class TaskReactorLogger extends PrefixLogger {

  private static final String PREFIX = "ConnectorsSDK::TaskReactor::";

  /**
   * Creates a new logger for a given class
   *
   * @param clazz source class
   * @return new logger instance
   */
  public static Logger getLogger(Class<?> clazz) {
    return new TaskReactorLogger(clazz);
  }

  private TaskReactorLogger(Class<?> clazz) {
    super(PREFIX, LoggerFactory.getLogger(clazz));
  }
}
