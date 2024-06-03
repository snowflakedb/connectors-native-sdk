/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Session;

/**
 * This class is a temporary solution for the problem with Snowflake Streams staleness. It will be
 * most probably deleted in the future versions of the SDK - be aware of it when using it.
 */
@Deprecated
public interface InstanceStreamService {

  /**
   * The method recreates task reactor instance streams to extend their staleness time.
   *
   * @param instanceName tha name of the Task Reactor instance.
   */
  void recreateStreams(Identifier instanceName);

  /**
   * The method recreate task reactor instance streams to extend their staleness time only when the
   * appropriate time passed since the last recreation.
   *
   * @param instanceName the name of the Task Reactor instance.
   */
  void recreateStreamsIfRequired(Identifier instanceName);

  /**
   * The method provides the default implementation of the instance stream service.
   *
   * @param session Snowpark session.
   * @return default instance stream service instance.
   */
  static InstanceStreamService getInstance(Session session) {
    return new DefaultInstanceStreamService(session);
  }
}
