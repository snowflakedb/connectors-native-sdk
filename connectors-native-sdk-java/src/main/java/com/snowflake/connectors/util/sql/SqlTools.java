/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Collection;
import java.util.List;
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
      return ConnectorResponse.fromVariant(
          callProcedureRaw(session, schema, procedureName, arguments));
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
   * Calls the specified application procedure with the provided arguments.
   *
   * <p>The procedure called must return a {@link Variant} which will be returned.
   *
   * @param session Snowpark session object
   * @param schema schema in which the procedure exists
   * @param procedureName procedure name
   * @param arguments procedure arguments
   * @return Variant representing response from a procedure.
   */
  public static Variant callProcedureRaw(
      Session session, String schema, String procedureName, String... arguments) {
    try {
      var procedureArguments = String.join(",", arguments);
      return session
          .sql(format("CALL %s.%s(%s)", schema, procedureName, procedureArguments))
          .collect()[0]
          .getVariant(0);
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
   * Calls the specified application procedure, created in the {@code PUBLIC} schema, with the
   * provided arguments.
   *
   * <p>The procedure called must return a {@link Variant} which will be returned
   *
   * @param session Snowpark session object
   * @param procedureName procedure name
   * @param arguments procedure arguments
   * @return Variant
   */
  public static Variant callPublicProcedureRaw(
      Session session, String procedureName, String... arguments) {
    return callProcedureRaw(session, "PUBLIC", procedureName, arguments);
  }

  /**
   * Returns the provided String wrapped in single quotes, so it can be treated as a SQL varchar.
   *
   * <p>If the provided String contains any single quote characters - they are escaped with a
   * backslash character.
   *
   * @param string String value
   * @return provided value escaped and wrapped in single quotes
   */
  public static String asVarchar(String string) {
    return string != null ? format("'%s'", string.replace("'", "\\'")) : null;
  }

  /**
   * Creates a String that wraps provided arguments in ARRAY_CONSTRUCT() function.
   *
   * @param objects String values that represent ARRAY_CONSTRUCT() function arguments.
   * @return provided values wrapped in ARRAY_CONSTRUCT() function.
   */
  public static String arrayConstruct(String... objects) {
    return arrayConstruct(List.of(objects));
  }

  /**
   * Creates a String that wraps provided arguments in ARRAY_CONSTRUCT() function.
   *
   * @param objects List of string values that represent ARRAY_CONSTRUCT() function arguments.
   * @return provided values wrapped in ARRAY_CONSTRUCT() function.
   */
  public static String arrayConstruct(List<String> objects) {
    String commaSeparatedArgs = String.join(",", objects);
    return format("ARRAY_CONSTRUCT(%s)", commaSeparatedArgs);
  }

  /**
   * Deprecated, use {@link #asVarchar(String)} instead.
   *
   * @param argument String value
   * @return provided value escaped and wrapped in single quotes
   */
  @Deprecated(since = "2.1.0", forRemoval = true)
  public static String varcharArgument(String argument) {
    return asVarchar(argument);
  }

  /**
   * Returns the provided Variant changed to a JSON String and wrapped in the {@code PARSE_JSON}
   * function, so it can be used as a SQL Variant.
   *
   * @param variant Variant value
   * @return provided value changed to a JSON String and wrapped in the {@code PARSE_JSON} function
   */
  public static String asVariant(Variant variant) {
    if (variant == null) {
      return null;
    }

    String escapedVariant = variant.asJsonString().replace("\\\"", "\\\\\\\"");
    return format("PARSE_JSON(%s)", asVarchar(escapedVariant));
  }

  /**
   * Deprecated, use {@link #asVariant(Variant)} instead.
   *
   * @param variant Variant value
   * @return provided value changed to a JSON String and wrapped in the {@code PARSE_JSON} function
   */
  @Deprecated(since = "2.1.0", forRemoval = true)
  public static String variantArgument(Variant variant) {
    return asVariant(variant);
  }

  /**
   * Quotes each string using {@link SqlTools#asVarchar(String) quote} and joins them into one
   * separated by commas.
   *
   * @param strings values to be joined
   * @return comma separated string
   */
  public static String asCommaSeparatedSqlList(Collection<String> strings) {
    return strings.stream().map(SqlTools::asVarchar).collect(Collectors.joining(","));
  }

  /**
   * Wraps input text with double quotes, e.g. input {@code test} will create output {@code "test"}.
   *
   * @param string string to be formatted
   * @return input string wrapped in double quotes
   */
  public static String quoted(String string) {
    return string != null ? format("\"%s\"", string) : null;
  }
}
