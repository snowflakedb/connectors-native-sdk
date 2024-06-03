/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

/** Utility class providing common string operations for SQL query purposes. */
public class SqlStringFormatter {

  private SqlStringFormatter() {}

  /**
   * Wraps input text with double quotes, e.g. input {@code test} will create output {@code "test"}.
   *
   * @param text string to be formatted
   * @return input string wrapped in double quotes
   */
  public static String quoted(String text) {
    return String.format("\"%s\"", text);
  }

  /**
   * Adds additional double quotes at the beginning and the end of the input. Additionally, for each
   * double quote found append second one before. Examples:
   *
   * <ul>
   *   <li>SOME-STRING$#@ -&gt; "SOME-STRING$#@"
   *   <li>SOME"STRING$#@ -&gt; "SOME""STRING$#@"
   *   <li>"SOME"STRING$#@" -&gt; """SOME""STRING$#@"""
   * </ul>
   *
   * @param text string to be formatted
   * @return escaped input string
   */
  public static String escapeIdentifier(String text) {
    var result = text.replace("\"", "\"\"");
    return String.format("\"%s\"", result);
  }
}
