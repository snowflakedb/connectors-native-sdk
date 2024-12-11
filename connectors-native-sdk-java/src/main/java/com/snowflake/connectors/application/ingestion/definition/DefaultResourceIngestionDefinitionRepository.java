/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.sysdate;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.util.sql.MergeStatementValidator;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Default implementation of {@link ResourceIngestionDefinitionRepository} for the given resource
 * ingestion definition.
 *
 * @param <R> resource ingestion definition class
 */
class DefaultResourceIngestionDefinitionRepository<
        R extends ResourceIngestionDefinition<?, ?, ?, ?>>
    implements ResourceIngestionDefinitionRepository<R> {

  /** Maximum limit of inserted/updated records. */
  public static final int EXPRESSION_LIMIT = 16384;

  private static final String TABLE_NAME = "state.resource_ingestion_definition";
  private static final String ID_COLUMN = "id";
  private static final String NAME_COLUMN = "name";
  private static final String ENABLED_COLUMN = "enabled";
  private static final String PARENT_ID_COLUMN = "parent_id";
  private static final String RESOURCE_ID_COLUMN = "resource_id";
  private static final String RESOURCE_METADATA_COLUMN = "resource_metadata";
  private static final String INGESTION_CONFIGURATION_COLUMN = "ingestion_configuration";
  private static final String UPDATED_AT_COLUMN = "updated_at";

  private static final StructType SCHEMA =
      StructType.create(
          new StructField(ID_COLUMN, DataTypes.StringType),
          new StructField(NAME_COLUMN, DataTypes.StringType),
          new StructField(ENABLED_COLUMN, DataTypes.BooleanType),
          new StructField(PARENT_ID_COLUMN, DataTypes.StringType),
          new StructField(RESOURCE_ID_COLUMN, DataTypes.VariantType),
          new StructField(RESOURCE_METADATA_COLUMN, DataTypes.VariantType),
          new StructField(INGESTION_CONFIGURATION_COLUMN, DataTypes.VariantType),
          new StructField(UPDATED_AT_COLUMN, DataTypes.TimestampType));

  private final Session session;
  private final ResourceIngestionDefinitionMapper<R, ?, ?, ?, ?> mapper;
  private final ResourceIngestionDefinitionValidator validator;

  DefaultResourceIngestionDefinitionRepository(
      Session session,
      ResourceIngestionDefinitionMapper<R, ?, ?, ?, ?> mapper,
      ResourceIngestionDefinitionValidator validator) {
    this.session = session;
    this.mapper = mapper;
    this.validator = validator;
  }

  @Override
  public Optional<R> fetch(String id) {
    return fetchFirst(col(ID_COLUMN).equal_to(lit(id)));
  }

  @Override
  public Optional<R> fetchByResourceId(Object resourceId) {
    return fetchFirst(
        col(RESOURCE_ID_COLUMN).equal_to(lit(mapToVariant(resourceId).asJsonString())));
  }

  private Optional<R> fetchFirst(Column condition) {
    return session
        .table(TABLE_NAME)
        .filter(condition)
        .select(
            ID_COLUMN,
            NAME_COLUMN,
            ENABLED_COLUMN,
            PARENT_ID_COLUMN,
            RESOURCE_ID_COLUMN,
            RESOURCE_METADATA_COLUMN,
            INGESTION_CONFIGURATION_COLUMN)
        .first()
        .map(mapper::map);
  }

  @Override
  public List<R> fetchAllById(List<String> ids) {
    return fetchAllWhere(col(ID_COLUMN).in(ids.toArray()));
  }

  @Override
  public List<R> fetchAllEnabled() {
    return fetchAllWhere(col(ENABLED_COLUMN).equal_to(lit(true)));
  }

  @Override
  public List<R> fetchAllWhere(Column condition) {
    return Arrays.stream(
            session
                .table(TABLE_NAME)
                .filter(condition)
                .select(
                    ID_COLUMN,
                    NAME_COLUMN,
                    ENABLED_COLUMN,
                    PARENT_ID_COLUMN,
                    RESOURCE_ID_COLUMN,
                    RESOURCE_METADATA_COLUMN,
                    INGESTION_CONFIGURATION_COLUMN)
                .collect())
        .map(mapper::map)
        .collect(toList());
  }

  @Override
  public long countEnabled() {
    return session.table(TABLE_NAME).filter(col(ENABLED_COLUMN).equal_to(lit(true))).count();
  }

  @Override
  public void save(R resource) {
    validator.validate(resource);

    Variant ingestionConfiguration = mapToVariant(resource.getIngestionConfigurations());
    Variant resourceId = mapToVariant(resource.getResourceId());
    Variant metadata = mapToVariant(resource.getResourceMetadata());

    var table = session.table(TABLE_NAME);
    var source =
        session.createDataFrame(
            new Row[] {
              Row.create(
                  resource.getId(),
                  resource.getName(),
                  resource.isEnabled(),
                  resource.getParentId(),
                  resourceId,
                  metadata,
                  ingestionConfiguration,
                  Timestamp.from(Instant.now()))
            },
            SCHEMA);

    var assignments =
        Map.of(
            col(ID_COLUMN), source.col(ID_COLUMN),
            col(NAME_COLUMN), source.col(NAME_COLUMN),
            col(ENABLED_COLUMN), source.col(ENABLED_COLUMN),
            col(PARENT_ID_COLUMN), source.col(PARENT_ID_COLUMN),
            col(RESOURCE_ID_COLUMN), source.col(RESOURCE_ID_COLUMN),
            col(RESOURCE_METADATA_COLUMN), source.col(RESOURCE_METADATA_COLUMN),
            col(INGESTION_CONFIGURATION_COLUMN), source.col(INGESTION_CONFIGURATION_COLUMN),
            col(UPDATED_AT_COLUMN), sysdate());

    var mergeResult =
        table
            .merge(source, source.col(ID_COLUMN).equal_to(table.col(ID_COLUMN)))
            .whenMatched()
            .update(assignments)
            .whenNotMatched()
            .insert(assignments)
            .collect();

    if (mergeResult.getRowsInserted() + mergeResult.getRowsUpdated() != 1) {
      throw new RuntimeException(
          "Invalid number of changes in table "
              + mergeResult.getRowsInserted()
              + " "
              + mergeResult.getRowsUpdated());
    }
  }

  /***
   * Method for creating or updating a list of resources. If object with resource#id does not exist, it will be created, otherwise the object will be updated.
   * When number of resources is greater than 50, it uses temporary table in order to merge new changes with existing table, otherwise it uses single merge query.
   * @throws ResourceIngestionDefinitionValidationException in case of validation error (Missing mandatory field or wrong scheduleDefinition value).
   */
  @Override
  public void saveMany(List<R> resources) {
    if (resources.isEmpty()) {
      return;
    }

    MergeStatementValidator.validateRecordLimit(resources);
    MergeStatementValidator.validateDuplicates(resources, R::getId);

    var query = prepareQuery(resources);
    executeQuery(query, resources);
  }

  @Override
  public void delete(String id) {
    session.table(TABLE_NAME).delete(col(ID_COLUMN).equal_to(lit(id)));
  }

  private String prepareQuery(Collection<R> resources) {
    var rows =
        resources.stream()
            .map(ingestionProcess -> "(?, ?, ?, ?, ?, ?, ?, ?)")
            .collect(joining(","));
    return String.format(
        "MERGE INTO %s AS target USING (SELECT id, name, enabled, parent_id,"
            + " PARSE_JSON(resource_id) AS resource_id, PARSE_JSON(resource_metadata) AS"
            + " resource_metadata, PARSE_JSON(ingestion_configuration) AS ingestion_configuration,"
            + " updated_at FROM (VALUES%s) AS V(id, name, enabled, parent_id, resource_id,"
            + " resource_metadata, ingestion_configuration, updated_at)) AS source ON target.id ="
            + " source.id WHEN MATCHED THEN UPDATE  SET target.name = source.name,  target.enabled"
            + " = source.enabled,  target.parent_id = source.parent_id,  target.resource_id ="
            + " source.resource_id,  target.resource_metadata = source.resource_metadata, "
            + " target.ingestion_configuration = source.ingestion_configuration,  target.updated_at"
            + " = source.updated_at WHEN NOT MATCHED THEN INSERT (ID, name,  enabled,"
            + " parent_id, resource_id, resource_metadata, ingestion_configuration,"
            + "  updated_at) VALUES (source.ID, source.name,  source.enabled, source.parent_id,"
            + " source.resource_id, source.resource_metadata,  source.ingestion_configuration,"
            + " source.updated_at) ",
        TABLE_NAME, rows);
  }

  private void executeQuery(String query, Collection<R> resources) {
    Connection connection = session.jdbcConnection();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(query);
      int i = 1;
      for (R resource : resources) {
        preparedStatement.setString(i++, resource.getId());
        preparedStatement.setString(i++, resource.getName());
        preparedStatement.setBoolean(i++, resource.isEnabled());
        preparedStatement.setString(i++, resource.getParentId());
        preparedStatement.setString(i++, mapToVariant(resource.getResourceId()).asString());
        preparedStatement.setString(i++, mapToVariant(resource.getResourceMetadata()).asString());
        preparedStatement.setString(
            i++, mapToVariant(resource.getIngestionConfigurations()).asString());
        preparedStatement.setTimestamp(i++, Timestamp.from(Instant.now()));
      }
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
