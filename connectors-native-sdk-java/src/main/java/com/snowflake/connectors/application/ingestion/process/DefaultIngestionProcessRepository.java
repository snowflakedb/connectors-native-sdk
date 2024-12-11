/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.sql.TimestampUtil.toInstant;
import static com.snowflake.connectors.util.sql.TimestampUtil.toTimestamp;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.row_number;
import static com.snowflake.snowpark_java.Functions.sysdate;
import static com.snowflake.snowpark_java.Window.partitionBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.util.sql.MergeStatementValidator;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.MergeResult;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.WindowSpec;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.StreamSupport;

/**
 * Default implementation of {@link IngestionProcessRepository} and {@link
 * CrudIngestionProcessRepository}.
 */
public class DefaultIngestionProcessRepository
    implements IngestionProcessRepository, CrudIngestionProcessRepository {

  /** Maximum limit of inserted/updated records. */
  public static final int EXPRESSION_LIMIT = 16384;

  private static final String TABLE_NAME = "state.ingestion_process";
  private static final String ID = "id";
  private static final String RESOURCE_INGESTION_DEFINITION_ID = "resource_ingestion_definition_id";
  private static final String INGESTION_CONFIGURATION_ID = "ingestion_configuration_id";
  private static final String TYPE = "type";
  private static final String STATUS = "status";
  private static final String CREATED_AT = "created_at";
  private static final String UPDATED_AT = "updated_at";
  private static final String FINISHED_AT = "finished_at";
  private static final String METADATA = "METADATA";

  private static final WindowSpec PARTITION_BY_TYPE =
      partitionBy(col(TYPE)).orderBy(col(FINISHED_AT).desc());
  private static final Column ROW_NUMBER_COL = row_number().over(PARTITION_BY_TYPE).as("ROW");

  private static final StructType ID_SCHEMA =
      StructType.create(new StructField(ID, DataTypes.StringType));

  private static final StructType COMPLEX_SCHEMA =
      StructType.create(
          new StructField(RESOURCE_INGESTION_DEFINITION_ID, DataTypes.StringType),
          new StructField(INGESTION_CONFIGURATION_ID, DataTypes.StringType),
          new StructField(TYPE, DataTypes.StringType));

  private final Session session;

  /**
   * Creates a new {@link DefaultIngestionProcessRepository}.
   *
   * @param session Snowpark session object
   */
  public DefaultIngestionProcessRepository(Session session) {
    this.session = session;
  }

  @Override
  public String createProcess(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status,
      Variant metadata) {
    var id = UUID.randomUUID().toString();

    var source = session.createDataFrame(new Row[] {Row.create(id)}, ID_SCHEMA);

    var assignments = new HashMap<Column, Column>();
    assignments.put(col(ID), lit(id));
    assignments.put(col(RESOURCE_INGESTION_DEFINITION_ID), lit(resourceIngestionDefinitionId));
    assignments.put(col(INGESTION_CONFIGURATION_ID), lit(ingestionConfigurationId));
    assignments.put(col(TYPE), lit(type));
    assignments.put(col(STATUS), lit(status));
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

  /**
   * {@inheritDoc} This implementation assumes that process cannot be in {@code FINISHED} status and
   * {@code finished_at} timestamp must be null to perform the update.
   *
   * @throws IngestionProcessUpdateException if no rows were updated or more than 1 row was updated
   */
  @Override
  public void updateStatus(String processId, String status) {
    var source = session.createDataFrame(new Row[] {Row.create(processId)}, ID_SCHEMA);

    var assignments =
        Map.of(
            col(STATUS), lit(status),
            col(UPDATED_AT), sysdate());

    var mergeResult =
        merge(source, assignments, source.col(ID).equal_to(session.table(TABLE_NAME).col(ID)));

    if (mergeResult.getRowsUpdated() > 1) {
      throw new IngestionProcessUpdateException(mergeResult.getRowsUpdated());
    }
  }

  /**
   * {@inheritDoc} This implementation assumes that process cannot be in {@code FINISHED} status and
   * {@code finished_at} timestamp must be null to perform the update.
   *
   * @throws IngestionProcessUpdateException if no rows were updated
   */
  @Override
  public void updateStatus(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status) {
    var source =
        session.createDataFrame(
            new Row[] {Row.create(resourceIngestionDefinitionId, ingestionConfigurationId, type)},
            COMPLEX_SCHEMA);

    var assignments =
        Map.of(
            col(STATUS), lit(status),
            col(UPDATED_AT), sysdate());

    var table = session.table(TABLE_NAME);

    merge(
        source,
        assignments,
        source
            .col(RESOURCE_INGESTION_DEFINITION_ID)
            .equal_to(table.col(RESOURCE_INGESTION_DEFINITION_ID))
            .and(
                source
                    .col(INGESTION_CONFIGURATION_ID)
                    .equal_to(table.col(INGESTION_CONFIGURATION_ID)))
            .and(source.col(TYPE).equal_to(table.col(TYPE))));
  }

  /**
   * {@inheritDoc} This implementation assumes that process cannot be in {@code FINISHED} status and
   * {@code finished_at} timestamp must be null to perform the update.
   *
   * @throws IngestionProcessUpdateException if no rows were updated or more than 1 row was updated
   */
  @Override
  public void endProcess(String processId) {
    var source = session.createDataFrame(new Row[] {Row.create(processId)}, ID_SCHEMA);

    var assignments =
        Map.of(
            col(STATUS), lit(FINISHED),
            col(UPDATED_AT), sysdate(),
            col(FINISHED_AT), sysdate());

    var table = session.table(TABLE_NAME);

    var mergeResult = merge(source, assignments, source.col(ID).equal_to(table.col(ID)));

    if (mergeResult.getRowsUpdated() > 1) {
      throw new IngestionProcessUpdateException(mergeResult.getRowsUpdated());
    }
  }

  /**
   * {@inheritDoc} This implementation assumes that process cannot be in {@code FINISHED} status and
   * {@code finished_at} timestamp must be null to perform the update.
   *
   * @throws IngestionProcessUpdateException if no rows were updated
   */
  @Override
  public void endProcess(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    var source =
        session.createDataFrame(
            new Row[] {Row.create(resourceIngestionDefinitionId, ingestionConfigurationId, type)},
            COMPLEX_SCHEMA);

    var assignments =
        Map.of(
            col(STATUS), lit(FINISHED),
            col(UPDATED_AT), sysdate(),
            col(FINISHED_AT), sysdate());

    var table = session.table(TABLE_NAME);

    merge(
        source,
        assignments,
        source
            .col(RESOURCE_INGESTION_DEFINITION_ID)
            .equal_to(table.col(RESOURCE_INGESTION_DEFINITION_ID))
            .and(
                source
                    .col(INGESTION_CONFIGURATION_ID)
                    .equal_to(table.col(INGESTION_CONFIGURATION_ID)))
            .and(source.col(TYPE).equal_to(table.col(TYPE))));
  }

  private MergeResult merge(
      DataFrame source, Map<Column, Column> assignments, Column joinExpression) {
    var table = session.table(TABLE_NAME);

    var mergeResult =
        table
            .merge(
                source,
                joinExpression
                    .and(table.col(STATUS).not_equal(lit(FINISHED)))
                    .and(table.col(FINISHED_AT).is_null()))
            .whenMatched()
            .update(assignments)
            .collect();

    if (mergeResult.getRowsUpdated() == 0) {
      throw new IngestionProcessUpdateException(mergeResult.getRowsUpdated());
    }

    return mergeResult;
  }

  @Override
  public Optional<IngestionProcess> fetch(String processId) {
    return fetch(col(ID).equal_to(lit(processId))).stream().findFirst();
  }

  @Override
  public List<IngestionProcess> fetchAllById(List<String> processIds) {
    return fetch(col(ID).in(processIds.toArray()));
  }

  @Override
  public Optional<IngestionProcess> fetchLastFinished(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    var where =
        col(RESOURCE_INGESTION_DEFINITION_ID)
            .equal_to(lit(resourceIngestionDefinitionId))
            .and(col(INGESTION_CONFIGURATION_ID).equal_to(lit(ingestionConfigurationId)))
            .and(col(TYPE).equal_to(lit(type)))
            .and(col(STATUS).equal_to(lit(FINISHED)));
    var sort = col(FINISHED_AT).desc();
    return fetch(where, sort).stream().findFirst();
  }

  @Override
  public List<IngestionProcess> fetchLastFinished(
      String resourceIngestionDefinitionId, String ingestionConfigurationId) {
    var where =
        col(RESOURCE_INGESTION_DEFINITION_ID)
            .equal_to(lit(resourceIngestionDefinitionId))
            .and(col(INGESTION_CONFIGURATION_ID).equal_to(lit(ingestionConfigurationId)))
            .and(col(STATUS).equal_to(lit(FINISHED)));

    var sort = col(FINISHED_AT).desc();
    return fetchDistinct(where, sort);
  }

  @Override
  public List<IngestionProcess> fetchAll(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    return fetch(
        col(RESOURCE_INGESTION_DEFINITION_ID)
            .equal_to(lit(resourceIngestionDefinitionId))
            .and(col(INGESTION_CONFIGURATION_ID).equal_to(lit(ingestionConfigurationId)))
            .and(col(TYPE).equal_to(lit(type))));
  }

  @Override
  public List<IngestionProcess> fetchAllActive(String resourceIngestionDefinitionId) {
    return fetch(
        col(RESOURCE_INGESTION_DEFINITION_ID)
            .equal_to(lit(resourceIngestionDefinitionId))
            .and(col(STATUS).in(SCHEDULED, IN_PROGRESS)));
  }

  @Override
  public void save(IngestionProcess process) {
    var source = session.createDataFrame(new Row[] {Row.create(process.getId())}, ID_SCHEMA);

    var assignments = new HashMap<Column, Column>();
    assignments.put(col(ID), lit(process.getId()));
    assignments.put(
        col(RESOURCE_INGESTION_DEFINITION_ID), lit(process.getResourceIngestionDefinitionId()));
    assignments.put(col(INGESTION_CONFIGURATION_ID), lit(process.getIngestionConfigurationId()));
    assignments.put(col(TYPE), lit(process.getType()));
    assignments.put(col(CREATED_AT), lit(toTimestamp(process.getCreatedAt())));
    assignments.put(col(FINISHED_AT), lit(toTimestamp(process.getFinishedAt())));
    assignments.put(col(STATUS), lit(process.getStatus()));
    if (process.getMetadata() != null) {
      assignments.put(col(METADATA), lit(process.getMetadata().asJsonString()));
    }

    var table = session.table(TABLE_NAME);
    table
        .merge(source, source.col(ID).equal_to(table.col(ID)))
        .whenNotMatched()
        .insert(assignments)
        .whenMatched()
        .update(assignments)
        .collect();
  }

  @Override
  public void save(Collection<IngestionProcess> ingestionProcesses) {
    if (ingestionProcesses.isEmpty()) {
      return;
    }

    MergeStatementValidator.validateRecordLimit(ingestionProcesses);
    MergeStatementValidator.validateDuplicates(ingestionProcesses, IngestionProcess::getId);

    var query = prepareQuery(ingestionProcesses);
    executeQuery(query, ingestionProcesses);
  }

  @Override
  public List<IngestionProcess> fetchAll(List<String> resourceIngestionDefinitionIds) {
    return fetch(
        col(RESOURCE_INGESTION_DEFINITION_ID).in(resourceIngestionDefinitionIds.toArray()));
  }

  @Override
  public List<IngestionProcess> fetchAll(String status) {
    return fetch(col(STATUS).equal_to(lit(status)));
  }

  @Override
  public void deleteAllByResourceId(String resourceIngestionDefinitionId) {
    session
        .table(TABLE_NAME)
        .delete(col(RESOURCE_INGESTION_DEFINITION_ID).equal_to(lit(resourceIngestionDefinitionId)));
  }

  private List<IngestionProcess> fetch(Column condition) {
    return fetch(condition, col(CREATED_AT).desc());
  }

  private List<IngestionProcess> fetchDistinct(Column condition, Column sort) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                session
                    .table(TABLE_NAME)
                    .select(
                        col(ID),
                        col(RESOURCE_INGESTION_DEFINITION_ID),
                        col(INGESTION_CONFIGURATION_ID),
                        col(TYPE),
                        col(STATUS),
                        col(CREATED_AT),
                        col(FINISHED_AT),
                        col(METADATA),
                        ROW_NUMBER_COL)
                    .where(condition)
                    .filter(col("ROW").equal_to(lit(1)))
                    .sort(sort)
                    .toLocalIterator(),
                Spliterator.ORDERED),
            false)
        .map(this::mapToIngestionProcess)
        .collect(toList());
  }

  private List<IngestionProcess> fetch(Column condition, Column sort) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                session
                    .table(TABLE_NAME)
                    .select(
                        ID,
                        RESOURCE_INGESTION_DEFINITION_ID,
                        INGESTION_CONFIGURATION_ID,
                        TYPE,
                        STATUS,
                        CREATED_AT,
                        FINISHED_AT,
                        METADATA)
                    .where(condition)
                    .sort(sort)
                    .toLocalIterator(),
                Spliterator.ORDERED),
            false)
        .map(this::mapToIngestionProcess)
        .collect(toList());
  }

  private IngestionProcess mapToIngestionProcess(Row row) {
    return new IngestionProcess(
        row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        toInstant(row.getTimestamp(5)),
        toInstant(row.getTimestamp(6)),
        Optional.ofNullable(row.get(7)).map(it -> row.getVariant(7)).orElse(null));
  }

  private String prepareQuery(Collection<IngestionProcess> ingestionProcesses) {
    var rows =
        ingestionProcesses.stream()
            .map(ingestionProcess -> "(?, ?, ?, ?, ?, ?, ?, ?)")
            .collect(joining(","));
    return String.format(
        "MERGE INTO %s AS target USING (SELECT ID, RESOURCE_INGESTION_DEFINITION_ID,"
            + " INGESTION_CONFIGURATION_ID, TYPE, STATUS, "
            + " try_to_timestamp_ntz(CREATED_AT) as CREATED_AT,  "
            + " try_to_timestamp_ntz(nvl(FINISHED_AT, '')) as FINISHED_AT, "
            + " PARSE_JSON(METADATA) AS METADATA FROM (VALUES%s) AS V(ID,"
            + " RESOURCE_INGESTION_DEFINITION_ID, INGESTION_CONFIGURATION_ID, TYPE, STATUS,"
            + " CREATED_AT, FINISHED_AT, METADATA)) AS source ON target.id = source.id WHEN MATCHED"
            + " THEN UPDATE  SET target.ID = source.ID,  target.RESOURCE_INGESTION_DEFINITION_ID ="
            + " source.RESOURCE_INGESTION_DEFINITION_ID,  target.INGESTION_CONFIGURATION_ID ="
            + " source.INGESTION_CONFIGURATION_ID,  target.TYPE = source.TYPE,  target.STATUS ="
            + " source.STATUS,  target.CREATED_AT = source.CREATED_AT,  target.FINISHED_AT ="
            + " source.FINISHED_AT,  target.METADATA = source.METADATA,  target.updated_at ="
            + " SYSDATE()  WHEN NOT MATCHED THEN INSERT (\"ID\","
            + " \"RESOURCE_INGESTION_DEFINITION_ID\",  \"INGESTION_CONFIGURATION_ID\", \"TYPE\","
            + " \"STATUS\", \"CREATED_AT\", \"FINISHED_AT\",  \"METADATA\", \"UPDATED_AT\") VALUES"
            + " (source.ID, source.RESOURCE_INGESTION_DEFINITION_ID, "
            + " source.INGESTION_CONFIGURATION_ID, source.TYPE, source.STATUS, source.CREATED_AT, "
            + " source.FINISHED_AT, source.METADATA, SYSDATE()) ",
        TABLE_NAME, rows);
  }

  private void executeQuery(String query, Collection<IngestionProcess> ingestionProcesses) {
    Connection connection = session.jdbcConnection();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(query);
      int i = 1;
      for (IngestionProcess ingestionProcess : ingestionProcesses) {
        preparedStatement.setString(i++, ingestionProcess.getId());
        preparedStatement.setString(i++, ingestionProcess.getResourceIngestionDefinitionId());
        preparedStatement.setString(i++, ingestionProcess.getIngestionConfigurationId());
        preparedStatement.setString(i++, ingestionProcess.getType());
        preparedStatement.setString(i++, ingestionProcess.getStatus());
        preparedStatement.setString(
            i++,
            String.valueOf(
                Optional.ofNullable(ingestionProcess.getCreatedAt())
                    .orElse(Instant.now())
                    .toEpochMilli()));
        preparedStatement.setString(
            i++,
            Optional.ofNullable(ingestionProcess.getFinishedAt())
                .map(Instant::toEpochMilli)
                .map(String::valueOf)
                .orElse(null));

        String metadata =
            ingestionProcess.getMetadata() == null
                ? null
                : ingestionProcess.getMetadata().asString();
        preparedStatement.setString(i++, metadata);
      }
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
