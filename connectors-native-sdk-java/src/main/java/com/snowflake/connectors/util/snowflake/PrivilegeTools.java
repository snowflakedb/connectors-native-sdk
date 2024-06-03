/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import com.snowflake.snowpark_java.Session;
import java.util.Arrays;

/** Set of basic privileges/grants utilities. */
public final class PrivilegeTools {

  private final Session session;

  public PrivilegeTools(Session session) {
    this.session = session;
  }

  /**
   * Returns whether the application instance has been granted the specified account privilege.
   *
   * @param privilege account privilege
   * @return whether the application instance has been granted the specified account privilege
   */
  public boolean hasPrivilege(String privilege) {
    return hasPrivilege(session, privilege);
  }

  /**
   * Validates whether the application instance has been granted the specified account privileges.
   *
   * @param privileges account privileges
   * @throws RequiredPrivilegesMissingException if any of the specified privileges was not granted
   */
  public void validatePrivileges(String... privileges) {
    validatePrivileges(session, privileges);
  }

  /**
   * Returns whether the application instance has been granted the specified account privilege.
   *
   * @param session Snowpark session object
   * @param privilege account privilege
   * @return whether the application instance has been granted the specified account privilege
   */
  public static boolean hasPrivilege(Session session, String privilege) {
    return session
        .sql("SELECT SYSTEM$HOLD_PRIVILEGE_ON_ACCOUNT('" + privilege + "')")
        .collect()[0]
        .getBoolean(0);
  }

  /**
   * Validates whether the application instance has been granted the specified account privileges.
   *
   * @param session Snowpark session object
   * @param privileges account privileges
   * @throws RequiredPrivilegesMissingException if any of the specified privileges was not granted
   */
  public static void validatePrivileges(Session session, String... privileges) {
    var missingPrivileges =
        Arrays.stream(privileges).filter(p -> !hasPrivilege(session, p)).toArray(String[]::new);

    if (missingPrivileges.length > 0) {
      throw new RequiredPrivilegesMissingException(missingPrivileges);
    }
  }
}
