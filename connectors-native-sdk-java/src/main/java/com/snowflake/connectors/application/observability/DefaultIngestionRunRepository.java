/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability;

import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.IN_PROGRESS;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.valueOf;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.sysdate;
import static java.util.stream.Collectors.toList;
import static net.snowflake.client.jdbc.internal.org.jsoup.internal.StringUtil.isBlank;

import com.snowflake.connectors.application.observability.exception.IngestionRunsBadStatusException;
import com.snowflake.connectors.application.observability.exception.IngestionRunsReferenceException;
import com.snowflake.connectors.application.observability.exception.IngestionRunsUpdateException;
import com.snowflake.connectors.util.sql.TimestampUtil;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.StreamSupport;

/**
 * Default implementation of {@link IngestionRunRepository} and {@link CrudIngestionRunRepository}.
 */
public class DefaultIngestionRunRepository
    implements IngestionRunRepository, CrudIngestionRunRepository {

  private static final String TABLE_NAME = "state.ingestion_run";
  private static final String ID = "id";
  private static final String RESOURCE_INGESTION_DEFINITION_ID = "resource_ingestion_definition_id";
  private static final String INGESTION_CONFIGURATION_ID = "ingestion_configuration_id";
  private static final String INGESTION_PROCESS_ID = "ingestion_process_id";
  private static final String STARTED_AT = "started_at";
  private static final String UPDATED_AT = "updated_at";
  private static final String COMPLETED_AT = "completed_at";
  private static final String STATUS = "status";
  private static final String INGESTED_ROWS = "ingested_rows";
  private static final String METADATA = "metadata";

  private static final StructType ID_SCHEMA =
      StructType.create(new StructField(ID, DataTypes.StringType));

  private static final StructType CRUD_SAVE_SCHEMA =
      StructType.create(
          new StructField(ID, DataTypes.StringType),
          new StructField(RESOURCE_INGESTION_DEFINITION_ID, DataTypes.StringType),
          new StructField(INGESTION_CONFIGURATION_ID, DataTypes.StringType),
          new StructField(INGESTION_PROCESS_ID, DataTypes.StringType),
          new StructField(STARTED_AT, DataTypes.TimestampType),
          new StructField(COMPLETED_AT, DataTypes.TimestampType),
          new StructField(UPDATED_AT, DataTypes.TimestampType),
          new StructField(STATUS, DataTypes.StringType),
          new StructField(INGESTED_ROWS, DataTypes.LongType),
          new StructField(METADATA, DataTypes.VariantType));

  private final Session session;

  /**
   * Creates a new {@link DefaultIngestionRunRepository}.
   *
   * @param session Snowpark session object
   */
  public DefaultIngestionRunRepository(Session session) {
    this.session = session;
  }

  @Override
  public String startRun(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String ingestionProcessId,
      Variant metadata) {
    if ((isBlank(resourceIngestionDefinitionId) || isBlank(ingestionConfigurationId))
        && isBlank(ingestionProcessId)) {
      throw new IngestionRunsReferenceException();
    }
    var id = UUID.randomUUID().toString();
    var source = session.createDataFrame(new Row[] {Row.create(id)}, ID_SCHEMA);
    var assignments = new HashMap<Column, Column>();
    assignments.put(col(ID), lit(id));
    assignments.put(col(RESOURCE_INGESTION_DEFINITION_ID), lit(resourceIngestionDefinitionId));
    assignments.put(col(INGESTION_CONFIGURATION_ID), lit(ingestionConfigurationId));
    assignments.put(col(INGESTION_PROCESS_ID), lit(ingestionProcessId));
    assignments.put(col(STATUS), lit(IN_PROGRESS.name()));

    if (metadata != null) {
      assignments.put(col(METADATA), lit(metadata.asJsonString()));
    }

    var table = session.table(TABLE_NAME);
    table
        .merge(source, source.col(ID).equal_to(table.col(ID)))
        .whenNotMatched()
        .insert(assignments)
        .collect();

    return id;
  }

  @Override
  public void endRun(
      String id,
      IngestionRun.IngestionStatus status,
      Long ingestedRows,
      Mode mode,
      Variant metadata) {

    if (status == IN_PROGRESS) {
      throw new IngestionRunsBadStatusException(status);
    }

    var assignments = new HashMap<Column, Column>();
    assignments.put(col(COMPLETED_AT), sysdate());
    assignments.put(col(STATUS), lit(status.name()));
    assignments.put(col(UPDATED_AT), sysdate());
    assignments.put(col(INGESTED_ROWS), lit(calculateIngestedRows(id, mode, ingestedRows)));
    if (metadata != null) {
      assignments.put(col(METADATA), lit(metadata.asJsonString()));
    }

    merge(id, assignments);
  }

  @Override
  public void updateIngestedRows(String id, Long ingestedRows, Mode mode) {
    var assignments =
        Map.of(
            col(INGESTED_ROWS),
            lit(calculateIngestedRows(id, mode, ingestedRows)),
            col(UPDATED_AT),
            sysdate());

    merge(id, assignments);
  }

  @Override
  public Optional<IngestionRun> findById(String id) {
    return findBy(col(ID).equal_to(lit(id)), col(ID).desc());
  }

  @Override
  public Optional<IngestionRun> findBy(Column condition, Column sort) {
    return session
        .table(TABLE_NAME)
        .select(
            col(RESOURCE_INGESTION_DEFINITION_ID),
            col(INGESTION_CONFIGURATION_ID),
            col(STATUS),
            col(STARTED_AT),
            col(COMPLETED_AT),
            col(INGESTED_ROWS),
            col(UPDATED_AT),
            col(INGESTION_PROCESS_ID),
            col(METADATA),
            col(ID))
        .filter(condition)
        .sort(sort)
        .first()
        .map(this::rowMapper);
  }

  @Override
  public List<IngestionRun> fetchAllByResourceId(String resourceIngestionDefinitionId) {
    return fetchAllBy(
        col(RESOURCE_INGESTION_DEFINITION_ID).equal_to(lit(resourceIngestionDefinitionId)));
  }

  @Override
  public List<IngestionRun> fetchAllByProcessId(String processId) {
    return fetchAllBy(col(INGESTION_PROCESS_ID).equal_to(lit(processId)));
  }

  @Override
  public void deleteAllByResourceId(String resourceIngestionDefinitionId) {
    session
        .table(TABLE_NAME)
        .delete(col(RESOURCE_INGESTION_DEFINITION_ID).equal_to(lit(resourceIngestionDefinitionId)));
  }

  @Override
  public void save(IngestionRun ingestionRun) {
    var source =
        session.createDataFrame(
            new Row[] {
              Row.create(
                  ingestionRun.getId(),
                  ingestionRun.getIngestionDefinitionId(),
                  ingestionRun.getIngestionConfigurationId(),
                  ingestionRun.getIngestionProcessId(),
                  TimestampUtil.toTimestamp(ingestionRun.getStartedAt()),
                  TimestampUtil.toTimestamp(ingestionRun.getCompletedAt()),
                  TimestampUtil.toTimestamp(
                      Optional.ofNullable(ingestionRun.getUpdatedAt()).orElse(Instant.now())),
                  ingestionRun.getStatus().name(),
                  ingestionRun.getIngestedRows(),
                  ingestionRun.getMetadata())
            },
            CRUD_SAVE_SCHEMA);

    var assignments = new HashMap<Column, Column>();
    assignments.put(col(ID), source.col(ID));
    assignments.put(
        col(RESOURCE_INGESTION_DEFINITION_ID), source.col(RESOURCE_INGESTION_DEFINITION_ID));
    assignments.put(col(INGESTION_CONFIGURATION_ID), source.col(INGESTION_CONFIGURATION_ID));
    assignments.put(col(INGESTION_PROCESS_ID), source.col(INGESTION_PROCESS_ID));
    if (ingestionRun.getStartedAt() != null) {
      assignments.put(col(STARTED_AT), source.col(STARTED_AT));
    }
    assignments.put(col(COMPLETED_AT), source.col(COMPLETED_AT));
    assignments.put(col(UPDATED_AT), source.col(UPDATED_AT));
    assignments.put(col(STATUS), source.col(STATUS));
    assignments.put(col(INGESTED_ROWS), source.col(INGESTED_ROWS));
    assignments.put(col(METADATA), source.col(METADATA));

    var table = session.table(TABLE_NAME);

    table
        .merge(source, source.col(ID).equal_to(table.col(ID)))
        .whenMatched()
        .update(assignments)
        .whenNotMatched()
        .insert(assignments)
        .collect();
  }

  @Override
  public List<IngestionRun> findWhere(Column condition) {
    return fetchAllBy(condition);
  }

  @Override
  public List<IngestionRun> findOngoingIngestionRuns() {
    return findOngoingIngestionRunsWhere(lit("1").equal_to(lit("1")));
  }

  @Override
  public List<IngestionRun> findOngoingIngestionRuns(String resourceIngestionDefinitionId) {
    return findOngoingIngestionRunsWhere(
        col(RESOURCE_INGESTION_DEFINITION_ID).equal_to(lit(resourceIngestionDefinitionId)));
  }

  @Override
  public List<IngestionRun> findOngoingIngestionRunsWhere(Column condition) {
    return fetchAllBy(
        col(STATUS)
            .equal_to(lit(IN_PROGRESS.name()))
            .and(col(COMPLETED_AT).is_null())
            .and(condition));
  }

  private List<IngestionRun> fetchAllBy(Column condition) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                session
                    .table(TABLE_NAME)
                    .select(
                        col(RESOURCE_INGESTION_DEFINITION_ID),
                        col(INGESTION_CONFIGURATION_ID),
                        col(STATUS),
                        col(STARTED_AT),
                        col(COMPLETED_AT),
                        col(INGESTED_ROWS),
                        col(UPDATED_AT),
                        col(INGESTION_PROCESS_ID),
                        col(METADATA),
                        col(ID))
                    .filter(condition)
                    .sort(col(STARTED_AT).desc())
                    .toLocalIterator(),
                Spliterator.ORDERED),
            false)
        .map(this::rowMapper)
        .collect(toList());
  }

  private void merge(String id, Map<Column, Column> assignments) {
    var source = session.createDataFrame(new Row[] {Row.create(id)}, ID_SCHEMA);
    var table = session.table(TABLE_NAME);

    var mergeResult =
        table
            .merge(source, source.col(ID).equal_to(table.col(ID)))
            .whenMatched()
            .update(assignments)
            .collect();

    if (mergeResult.getRowsUpdated() != 1) {
      throw new IngestionRunsUpdateException(mergeResult.getRowsUpdated());
    }
  }

  private Long calculateIngestedRows(String id, Mode mode, Long ingestedRows) {
    var table = session.table(TABLE_NAME);

    if (mode == Mode.ADD) {
      return Arrays.stream(
              table.select(col(INGESTED_ROWS), col(ID)).filter(col(ID).equal_to(lit(id))).collect())
          .map(row -> row.getLong(0))
          .findFirst()
          .map(it -> it + ingestedRows)
          .orElse(ingestedRows);
    } else {
      return ingestedRows;
    }
  }

  private IngestionRun rowMapper(Row row) {
    return new IngestionRun(
        row.getString(9),
        row.getString(0),
        row.getString(1),
        valueOf(row.getString(2)),
        TimestampUtil.toInstant(row.getTimestamp(3)),
        TimestampUtil.toInstant(row.getTimestamp(4)),
        row.getLong(5),
        TimestampUtil.toInstant(row.getTimestamp(6)),
        row.getString(7),
        Optional.ofNullable(row.get(8)).map(it -> row.getVariant(8)).orElse(null));
  }
}
