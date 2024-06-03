/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import static java.lang.String.format;

import com.snowflake.snowpark_java.Session;

// TODO("CONN-7402")
public class SqlHelper {

  public static String prepareProcedureCallStatement(
      String schema, String procedureName, String... arguments) {
    String procedureArguments = String.join(",", arguments);
    return format("CALL %s.%s(%s)", schema, procedureName, procedureArguments);
  }

  public static void callProcedure(
      Session session, String schema, String procedureName, String... arguments) {
    session.sql(prepareProcedureCallStatement(schema, procedureName, arguments)).collect();
  }

  public static String varcharArg(String argument) {
    return format("'%s'", argument);
  }
}
