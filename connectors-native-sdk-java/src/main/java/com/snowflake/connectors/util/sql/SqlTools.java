/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Collection;
import java.util.stream.Collectors;

/** Set of basic SQL utilities. */
public class SqlTools {

  /**
   * Calls the specified application procedure with the provided arguments.
   *
   * <p>The procedure called must return a {@link Variant}, which must be possible to map into a
   * {@link ConnectorResponse} instance.
   *
   * @param session Snowpark session object
   * @param schema schema in which the procedure exists
   * @param procedureName procedure name
   * @param arguments procedure arguments
   * @return standard connector response created from the Variant returned by the procedure
   */
  public static ConnectorResponse callProcedure(
      Session session, String schema, String procedureName, String... arguments) {
    try {
      var procedureArguments = String.join(",", arguments);
      var variantResponse =
          session
              .sql(String.format("CALL %s.%s(%s)", schema, procedureName, procedureArguments))
              .collect()[0]
              .getVariant(0);
      return ConnectorResponse.fromVariant(variantResponse);
    } catch (ConnectorException exception) {
      throw exception;
    } catch (Exception exception) {
      if (exception.getMessage().contains("Unknown user-defined function")) {
        throw new NonexistentProcedureCallException(procedureName);
      } else {
        throw new UnknownSqlException(procedureName, exception.getMessage());
      }
    }
  }

  /**
   * Calls the specified application procedure, created in the {@code PUBLIC} schema, with the
   * provided arguments.
   *
   * <p>The procedure called must return a {@link Variant}, which must be possible to map into a
   * {@link ConnectorResponse} instance.
   *
   * @param session Snowpark session object
   * @param procedureName procedure name
   * @param arguments procedure arguments
   * @return standard connector response created from the Variant returned by the procedure
   */
  public static ConnectorResponse callPublicProcedure(
      Session session, String procedureName, String... arguments) {
    return callProcedure(session, "PUBLIC", procedureName, arguments);
  }

  /**
   * Returns the provided String wrapped in single quotes, so it can be treated as a SQL varchar.
   *
   * @param argument String value
   * @return provided value wrapped in single quotes
   */
  public static String varcharArgument(String argument) {
    return String.format("'%s'", argument);
  }

  /**
   * Returns the provided Variant changed to a JSON String and wrapped in the {@code PARSE_JSON}
   * function, so it can be used as a SQL Variant.
   *
   * @param variant Variant value
   * @return provided value changed to a JSON String and wrapped in the {@code PARSE_JSON} function
   */
  public static String variantArgument(Variant variant) {
    String escapedVariant = variant.asJsonString().replaceAll("\"", "\\\\\"");
    return String.format("PARSE_JSON('%s')", escapedVariant);
  }

  /**
   * Quotes each string using {@link SqlTools#varcharArgument(String) quote} and joins them into one
   * separated by commas.
   *
   * @param values values to be joined
   * @return comma separated string
   */
  public static String asCommaSeparatedSqlList(Collection<String> values) {
    return values.stream().map(SqlTools::varcharArgument).collect(Collectors.joining(","));
  }
}
