/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.http;

import static java.util.Arrays.stream;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simple parser for Link header. This is NOT a complete implementation of
 * https://www.rfc-editor.org/rfc/rfc5988.html
 */
public final class LinkHeaderParser {

  private LinkHeaderParser() {}

  /**
   * Parse headers to find relative page data.
   *
   * @see <a
   *     href="https://docs.github.com/en/rest/guides/using-pagination-in-the-rest-api?apiVersion=2022-11-28#using-link-headers">GitHub
   *     API documentation</a>
   * @param linkHeader Value of the link header
   * @return A map containing all parsed data
   */
  public static Map<String, String> parseLink(String linkHeader) {
    return stream(linkHeader.split(","))
        .map(String::trim)
        .map(
            it -> {
              var split = it.split(";");
              var url = split[0].substring(1, split[0].length() - 1);
              var rawRel = split[1].trim().split("=")[1];
              var rel = rawRel.trim().substring(1, rawRel.length() - 1);
              return new AbstractMap.SimpleImmutableEntry<>(rel, url);
            })
        .collect(
            Collectors.toMap(
                AbstractMap.SimpleImmutableEntry::getKey,
                AbstractMap.SimpleImmutableEntry::getValue));
  }
}
