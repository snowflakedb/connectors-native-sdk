/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors;

public class ApplicationVersion {

  private final String sdkVersion;
  private final String appPackageVersion;

  public ApplicationVersion(String sdkVersion, String appPackageVersion) {
    this.sdkVersion = sdkVersion;
    this.appPackageVersion = appPackageVersion;
  }

  public String getSdkVersion() {
    return sdkVersion;
  }

  public String getAppPackageVersion() {
    return appPackageVersion;
  }
}
