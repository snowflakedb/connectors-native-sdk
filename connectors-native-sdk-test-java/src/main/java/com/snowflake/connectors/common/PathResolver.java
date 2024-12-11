/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common;

import java.nio.file.Path;
import java.nio.file.Paths;

/** A simple utility for resolving paths based on the home dir or current work dir. */
public class PathResolver {

  private static final String USER_DIR = System.getProperty("user.dir");
  private static final String USER_HOME_DIR = System.getProperty("user.home");

  /**
   * Creates {@link Path} based on the input string. Path staring with {@code ~/} is interpreted as
   * path from USER.HOME system property.
   *
   * @param path string path
   * @return path
   */
  public static Path resolve(String path) {
    if (path.startsWith("~/")) {
      var relativePath = path.replaceFirst("~/", "");
      return Path.of(USER_HOME_DIR, relativePath.split("/"));
    }

    if (path.startsWith("/")) {
      return Paths.get(path);
    }

    return Path.of(USER_DIR, path.split("/"));
  }
}
