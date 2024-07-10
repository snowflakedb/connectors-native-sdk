/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.SchemaName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In memory implementation of {@link AccessTools}. */
public class InMemoryAccessTools implements AccessTools {

  private final List<String> warehouses;
  private final Map<String, List<String>> databasesAndSchemas;
  private final List<String> secrets;

  public InMemoryAccessTools() {
    warehouses = new ArrayList<>();
    databasesAndSchemas = new HashMap<>();
    secrets = new ArrayList<>();
  }

  @Override
  public boolean hasWarehouseAccess(Identifier warehouse) {
    return warehouses.contains(warehouse.getUnquotedValue());
  }

  @Override
  public boolean hasDatabaseAccess(Identifier database) {
    return databasesAndSchemas.containsKey(database.getUnquotedValue());
  }

  @Override
  public boolean hasSchemaAccess(SchemaName schemaName) {
    Identifier database = schemaName.getDatabase().orElseThrow();

    if (!hasDatabaseAccess(database)) {
      return false;
    }

    List<String> schemas = databasesAndSchemas.get(database.getUnquotedValue());

    return (schemas != null) && schemas.contains(schemaName.getSchema().getUnquotedValue());
  }

  @Override
  public boolean hasSecretAccess(ObjectName secret) {
    return secrets.contains(secret.getValue());
  }

  /**
   * Adds warehouse to local store. In memory warehouse store is used in method {@link
   * #hasWarehouseAccess(Identifier)} to check if parameter is in it.
   *
   * @param warehouses accessible warehouse names
   */
  public void addWarehouses(String... warehouses) {
    this.warehouses.addAll(List.of(warehouses));
  }

  /**
   * Adds databases to local store. In memory database store is used in method {@link
   * #hasDatabaseAccess(Identifier)} to check if parameter is in it.
   *
   * @param databases accessible database names
   */
  public void addDatabases(String... databases) {
    Arrays.stream(databases)
        .forEach(database -> this.databasesAndSchemas.putIfAbsent(database, new ArrayList<>()));
  }

  /**
   * Adds schemas to in memory store. In memory schema store is used in method {@link
   * #hasSchemaAccess(SchemaName)} to check if parameter is in it.
   *
   * @param database accessible database name
   * @param schemas accessible schemas name
   */
  public void addSchemasInDatabase(String database, String... schemas) {
    if (databasesAndSchemas.containsKey(database)) {
      databasesAndSchemas.merge(
          database,
          List.of(schemas),
          (initial, appended) -> {
            initial.addAll(appended);
            return initial;
          });
    } else {
      databasesAndSchemas.put(database, new ArrayList<>(List.of(schemas)));
    }
  }

  /**
   * Adds secrets to in memory store. In memory secret store is used in method {@link
   * #hasSecretAccess(ObjectName)} to check if parameter is in it.
   *
   * @param secrets accessible secrets
   */
  public void addSecrets(String... secrets) {
    this.secrets.addAll(List.of(secrets));
  }
}
