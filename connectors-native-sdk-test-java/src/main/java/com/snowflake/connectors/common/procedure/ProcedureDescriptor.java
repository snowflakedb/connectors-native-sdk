package com.snowflake.connectors.common.procedure;

import static java.lang.String.format;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;

/** A simple utility used for describing stored procedures. */
public class ProcedureDescriptor {

  private static final String EAI_PROPERTY = "external_access_integrations";
  private static final String SECRET_PROPERTY = "secrets";
  private final Session session;

  /**
   * Creates a new {@link ProcedureDescriptor}.
   *
   * @param session Snowpark session object
   */
  public ProcedureDescriptor(Session session) {
    this.session = session;
  }

  /**
   * Describes the specified stored procedure.
   *
   * @param procedureName name of the stored procedure object
   * @param argumentTypes procedure argument types
   * @return procedure properties
   */
  public ProcedureProperties describeProcedure(ObjectName procedureName, String... argumentTypes) {
    String commaSeparatedArgTypes = String.join(",", argumentTypes);
    Row[] descResult =
        session
            .sql(
                format(
                    "DESCRIBE PROCEDURE %s(%s)", procedureName.getValue(), commaSeparatedArgTypes))
            .collect();
    return mapDescResult(descResult, procedureName);
  }

  private ProcedureProperties mapDescResult(Row[] descResult, ObjectName objectName) {
    var descResultStream = Arrays.stream(descResult);
    var externalAccessIntegration =
        descResultStream
            .filter(row -> EAI_PROPERTY.equals(row.getString(0)))
            .map(row -> row.getString(1))
            .findFirst();
    var secretValue =
        Arrays.stream(descResult)
            .filter(row -> SECRET_PROPERTY.equals(row.getString(0)))
            .map(row -> row.getString(1))
            .findFirst();
    return new ProcedureProperties(
        objectName, externalAccessIntegration.orElse(""), secretValue.orElse(""));
  }
}
