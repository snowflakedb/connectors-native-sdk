/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * This is an example test of upgrade and downgrade using specific versions of SDK. Please note that
 * this test just checks the same test application with different SDK versions and is not ready to
 * check different versions of custom code that can be provided as a part of a connector
 * application.
 */
@Disabled(
    "This is example test for upgrade and downgrade using specific SDK versions. We don't want to"
        + " run during every build because we have another test for latest release and changes from"
        + " branch versions.")
public class ConfigurableVersionsUpgradeDowngradeTest
    extends ConfigurableVersionsUpgradeDowngradeBaseTest {
  @Override
  protected ApplicationVersion firstApplicationVersion() {
    return new ApplicationVersion("2.0.0", "1_0");
  }

  @Override
  protected ApplicationVersion secondApplicationVersion() {
    return new ApplicationVersion("2.0.1", "2_0");
  }

  @Test
  void shouldUpgradeAndDowngradeApplication() {
    application.alterVersion(secondApplicationVersion().getAppPackageVersion());
    application.alterVersion(firstApplicationVersion().getAppPackageVersion());
  }
}
