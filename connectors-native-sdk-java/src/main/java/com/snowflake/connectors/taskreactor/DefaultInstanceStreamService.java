/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.taskreactor.ComponentNames.COMMANDS_QUEUE_STREAM;
import static com.snowflake.connectors.taskreactor.ComponentNames.CONFIG_TABLE;
import static com.snowflake.connectors.taskreactor.ComponentNames.QUEUE_STREAM;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Session;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Default implementation of {@link InstanceStreamService}. */
class DefaultInstanceStreamService implements InstanceStreamService {

  private static final int RECREATE_AFTER_DAYS = 88;
  private static final String LAST_STREAMS_RECREATION_KEY = "LAST_STREAMS_RECREATION";
  private final Session session;

  DefaultInstanceStreamService(Session session) {
    this.session = session;
  }

  @Override
  public void recreateStreamsIfRequired(Identifier instanceName) {
    if (shouldRecreate(instanceName)) {
      recreateStreams(instanceName);
    }
  }

  @Override
  public void recreateStreams(Identifier instanceName) {
    updateLastStreamsRecreationTime(instanceName);
    recreateStream(ObjectName.from(instanceName.toSqlString(), QUEUE_STREAM));
    recreateStream(ObjectName.from(instanceName.toSqlString(), COMMANDS_QUEUE_STREAM));
  }

  private void recreateStream(ObjectName streamName) {
    session
        .sql(
            String.format(
                "CREATE OR REPLACE STREAM %s CLONE %s",
                streamName.getEscapedName(), streamName.getEscapedName()))
        .collect();
  }

  private boolean shouldRecreate(Identifier instanceName) {
    var lastStreamsRecreation =
        Timestamp.valueOf(
            session
                .sql(
                    String.format(
                        "SELECT VALUE FROM %s.%s WHERE KEY = '%s'",
                        instanceName.toSqlString(), CONFIG_TABLE, LAST_STREAMS_RECREATION_KEY))
                .collect()[0]
                .getString(0));
    return Timestamp.from(Instant.now().minus(RECREATE_AFTER_DAYS, ChronoUnit.DAYS))
        .after(lastStreamsRecreation);
  }

  private void updateLastStreamsRecreationTime(Identifier instanceName) {
    session
        .sql(
            String.format(
                "UPDATE %s.%s SET VALUE = (SYSDATE()) WHERE KEY = '%s'",
                instanceName.toSqlString(), CONFIG_TABLE, LAST_STREAMS_RECREATION_KEY))
        .collect();
  }
}
