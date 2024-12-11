# Gradle connector lifecycle tasks:

---
## Prerequisites
1. All sql queries are executed with the usage of [Snowflake CLI tool](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index), so it's required to have this tool installed.
2. Define connection to Snowflake in the configuration file of the Snowflake CLI, which will be used in the tasks below [how to](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/connecting/specify-credentials).

## Terminology
- `connector build directory` - it's a directory where all connector artifacts should be stored before the deployment to Snowflake.
- `version directory` - it's a directory in the stage of the application package in Snowflake, which stores all connector artifacts. Application instance or version is created basing on this directory.

## One command build and deploy
Provided scripts contain convenience scripts that will handle building and deployment of the connector using just a single command. For the development purposes it is recommended to use the following command:
```shell
make reinstall_application_from_version_dir
```
This command will clean up previously deployed application, build any custom streamlit and java code, merge it with the Snowflake Native SDK for Connectors components and then create `APPLICATION PACKAGE` and `APPLICATION` in Snowflake.

Alternatively, there is another similar command, with the only difference being the creation of a `VERSION` inside the `APPLICATION PACKAGE`:
```shell
make reinstall_application_from_app_version
```

For more infor about each of the executed subcommands read below:

---
## 1. copyInternalComponents
This task builds jar artifact from local Java project and copies it to the connector build directory. It also copies all files from the source directory (like `*.sql` files) to the connector build directory.
Generally this task is intended to copy only local files created by the user. By default, it uses `./app` directory as a source directory and `./sf_build` as a target directory.

### OPTIONAL parameters:
- `srcDir` defines the path of the source directory to copy files from. Default value is `./app`, which is recommended directory name to store application files like `*.sql` files, `manifest.yml`, streamlit files, etc.
- `targetDir` defines the path to the target directory, to where files from the `srcDir` should be copied. This directory should store all files that should be deployed to Snowflake as application artifacts. Default value is `./sf_build`.
- `artifact` defines the name of the jar file built from the local Java project that is copied to the connector build directory. Default value is `<gradle_project_name>.jar`.

### Example command:
`./gradlew copyInternalComponents
-PsrcDir="./customSrcDir"
-PtargetDir="./customTargetDir"
-Partifact="custom-artifact-name.jar"`

---
## 2. copySdkComponents
This task copies the `connectors-sdk-components` directory which contains all `*.sql` files of the native-sdk features from the
`connectors-native-sdk-java` library (jar file). This task uses the `connectors-native-sdk-java` library from local Maven
repository (`./m2/repository`). The library will be downloaded by gradle from the Maven central repository, before it's used, so it will be present in the local files.

### OPTIONAL parameters:
- `targetDir` defines the path to the target directory, to where files from the `srcDir` should be copied. This directory should store all files that should be deployed to Snowflake as application artifacts. Default value is `./sf_build`.

### Example command:
`./gradlew copySdkComponents
-PtargetDir="./customTargetDir"`

---
## 3. prepareAppPackage
This task creates `APPLICATION PACKAGE`, `SCHEMA` and `STAGE` which will be the target for connector artifacts deployment.
In case, when the `APPLICATION PACKAGE` with the same name already exists, the task will fail.

### REQUIRED parameters
- `appPackage` - defines the name of the APPLICATION PACKAGE that will be created.
- `schema` - defines the name of the SCHEMA that will be created.
- `stage` - defines the name of the STAGE that will be created.
- `connectionFile` - defines the path to the connection config file which is used by the Snowflake CLI.

### Example command:
`./gradlew prepareAppPackage
-PappPackage="EXAMPLE_APP_PACKAGE"
-Pschema="EXAMPLE_SCHEMA"
-Pstage="EXAMPLE_STAGE"
-Pconnection="example_connection_name"`

---
## 4. deployConnector
This task puts all files which are stored in the connector build directory into the given stage of the application package.

### REQUIRED parameters
- `appPackage` - defines the name of the APPLICATION PACKAGE, to where, connector artifacts should be deployed.
- `schema` - defines the name of the SCHEMA, where the stage for artifacts is created.
- `stage` - defines the name of the STAGE created for storing connector artifacts.
- `connectionFile` - defines the path to the connection config file which is used by the Snowflake CLI.

### OPTIONAL parameters:
- `buildDirPath` - defines the path to the connector build directory from which, files will be put into the stage (the same as `targetDir` parameter in copy components tasks). Default value is `./sf_build`.
- *`appVersion` - defines the name of the version directory which will be created in the stage for storing connector artifact files. If this argument isn't provided, the default value is the version from the manifest file, but using this version will require user agreement (made with the usage of console input). If the version of the manifest file shouldn't be taken, the task finishes its execution gracefully.

### Example command:
`./gradlew deployConnector
-PappPackage="EXAMPLE_APP_PACKAGE"
-Pschema="EXAMPLE_SCHEMA"
-Pstage="EXAMPLE_STAGE"
-Pconnection="example_connection_name"
-PbuildDirPath="./example_build_dir"
-PappVersion="1_0"`

---
## 5. createNewVersion
This task creates new VERSION of the application from the given version directory.

### REQUIRED parameters
- `appPackage` - defines the name of the APPLICATION PACKAGE, from which, the new version should be created.
- `connectionFile` - defines the path to the connection config file which is used by the Snowflake CLI.
- `versionDirPath` - defines the absolute path to the version directory in the application package stage, for example `@EXAMPLE_APP.EXAMPLE_SCHEMA.EXAMPLE_STAGE/artifacts/v_1_0`.
- `appVersion` - defines the name of the application version that is created in the application package.

### Example command:
`./gradlew deployConnector
-PappPackage="EXAMPLE_APP_PACKAGE"
-Pconnection="example_connection_name"
-PversionDirPath="@EXAMPLE_APP.EXAMPLE_SCHEMA.EXAMPLE_STAGE/example_version_dir"
-PappVersion="1_0"`

---
## 6. createAppInstance
This task creates an instance of the application from the created version or directly from the version directory.
In case, when the `APPLICATION INSTANCE` with the same name already exists, the task will fail.

### REQUIRED parameters
- `appPackage` - defines the name of the APPLICATION PACKAGE, from which, the new application instance should be created.
- `connectionFile` - defines the path to the connection config file which is used by the Snowflake CLI.

### OPTIONAL parameters:
- `instanceName` - defines the name of the application instance that is created by the task. By default, the name of the instance is `<app_package_name>_INSTANCE`.
- `instanceNameSuffix` - defines the suffix added to the default instance name, when the custom instance name isn't specified.
- *`versionDirPath` - defines the absolute path to the version directory in the application package stage, from which the new instance should be created. Example path `@EXAMPLE_APP.EXAMPLE_SCHEMA.EXAMPLE_STAGE/artifacts/v_1_0`.
- *`appVersion` - defines the version name from which the application instance should be created. This parameter takes the priority over `versionDirPath` and when they are both provided the application instance is created from the version. In case, when both `appVersion` and `versionDirPath` arguments aren't provided, the task fails.

### Example command:
`./gradlew deployConnector
-PappPackage="EXAMPLE_APP_PACKAGE"
-Pconnection="example_connection_name"
-PversionDirPath="@EXAMPLE_APP.EXAMPLE_SCHEMA.EXAMPLE_STAGE/example_version_dir"
-PinstanceName="MY_EXAMPLE_CONNECTOR"`
