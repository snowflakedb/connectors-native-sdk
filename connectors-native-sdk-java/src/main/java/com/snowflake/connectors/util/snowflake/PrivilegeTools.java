/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import com.snowflake.snowpark_java.Session;

/** Set of basic privileges/grants utilities. */
public interface PrivilegeTools {

  /**
   * Returns whether the application instance has been granted the specified account privilege.
   *
   * @param privilege account privilege
   * @return whether the application instance has been granted the specified account privilege
   */
  boolean hasPrivilege(String privilege);

  /**
   * Validates whether the application instance has been granted the specified account privileges.
   *
   * @param privileges account privileges
   * @throws RequiredPrivilegesMissingException if any of the specified privileges was not granted
   */
  void validatePrivileges(String... privileges);

  /**
   * Returns a new instance of the default tools implementation.
   *
   * @param session Snowpark session object
   * @return a new tools instance
   */
  static PrivilegeTools getInstance(Session session) {
    return new DefaultPrivilegeTools(session);
  }
}
