/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * Represents properties of the table object as returned by the "SHOW TABLES ..." expression
 *
 * <p>Note: the list of properties is not complete.
 */
public class TableProperties {

  private final ObjectName objectName;
  private final Timestamp createdOn;
  private final String kind;
  private final String owner;

  /**
   * Creates new {@link TableProperties}
   *
   * @param database database
   * @param schema schema
   * @param name name of the table
   * @param createdOn created on property
   * @param kind kind
   * @param owner owner
   */
  public TableProperties(
      Identifier database,
      Identifier schema,
      Identifier name,
      Timestamp createdOn,
      String kind,
      String owner) {
    this.objectName = ObjectName.from(database, schema, name);
    this.createdOn = createdOn;
    this.kind = kind;
    this.owner = owner;
  }

  /**
   * {@link #objectName} property accessor
   *
   * @return objectName property
   */
  public ObjectName getObjectName() {
    return objectName;
  }

  /**
   * Returns String name of the table
   *
   * @return returns String name of the table as returned from SHOW TABLES ...
   */
  public String getName() {
    return objectName.getName().getValue();
  }

  /**
   * {@link #createdOn} property accessor
   *
   * @return createdOn property as returned from SHOW TABLES ...
   */
  public Timestamp getCreatedOn() {
    return createdOn;
  }

  /**
   * {@link #kind} property accessor
   *
   * @return kind property as returned from SHOW TABLES ...
   */
  public String getKind() {
    return kind;
  }

  /**
   * {@link #owner} property accessor
   *
   * @return name of the owner as returned from SHOW TABLES ...
   */
  public String getOwner() {
    return owner;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableProperties that = (TableProperties) o;
    return objectName.equals(that.objectName)
        && Objects.equals(kind, that.kind)
        && Objects.equals(owner, that.owner);
  }

  @Override
  public int hashCode() {
    int result = objectName.hashCode();
    result = 31 * result + Objects.hashCode(kind);
    result = 31 * result + Objects.hashCode(owner);
    return result;
  }
}
