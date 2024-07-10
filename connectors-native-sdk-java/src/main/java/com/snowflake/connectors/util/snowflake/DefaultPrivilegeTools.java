/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static java.lang.String.format;
import static java.util.function.Predicate.not;

import com.snowflake.snowpark_java.Session;
import java.util.Arrays;

/** Default implementation of {@link PrivilegeTools} */
final class DefaultPrivilegeTools implements PrivilegeTools {

  private final Session session;

  DefaultPrivilegeTools(Session session) {
    this.session = session;
  }

  /**
   * Returns whether the application instance has been granted the specified account privilege.
   *
   * @param privilege account privilege
   * @return whether the application instance has been granted the specified account privilege
   */
  @Override
  public boolean hasPrivilege(String privilege) {
    return session
        .sql(format("SELECT SYSTEM$HOLD_PRIVILEGE_ON_ACCOUNT(%s)", asVarchar(privilege)))
        .collect()[0]
        .getBoolean(0);
  }

  /**
   * Validates whether the application instance has been granted the specified account privileges.
   *
   * @param privileges account privileges
   * @throws RequiredPrivilegesMissingException if any of the specified privileges was not granted
   */
  @Override
  public void validatePrivileges(String... privileges) {
    var missingPrivileges =
        Arrays.stream(privileges).filter(not(this::hasPrivilege)).toArray(String[]::new);

    if (missingPrivileges.length > 0) {
      throw new RequiredPrivilegesMissingException(missingPrivileges);
    }
  }
}
