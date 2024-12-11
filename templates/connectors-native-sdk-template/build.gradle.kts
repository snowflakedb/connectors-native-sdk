import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.util.*

plugins {
    id("java")
}

group = "com.snowflake"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    compileOnly("com.snowflake:connectors-native-sdk:2.2.1-SNAPSHOT")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testImplementation("com.snowflake:connectors-native-sdk-test:2.2.1-SNAPSHOT")
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        links(
            "https://docs.oracle.com/en/java/javase/11/docs/api/",
            "https://docs.snowflake.com/developer-guide/snowpark/reference/java/",
            'https://www.javadoc.io/doc/com.fasterxml.jackson.core/jackson-databind/2.13.4/index.html'
        )
    }
}

tasks.test {
    useJUnitPlatform()
}

/**
 * Copyright (c) 2024 Snowflake Inc.
 *
 * **********************************************
 *           CONNECTOR LIFECYCLE TASKS
 * **********************************************
 */

val defaultBuildDir: String = "./sf_build"
val defaultSrcDir: String = "./app"
val libraryName: String = "connectors-native-sdk"
val defaultArtifactName: String = "${project.name}.jar"
val sdkComponentsDirName: String = "connectors-sdk-components"

tasks {
    register("copyInternalComponents") {
        group = "Snowflake"
        description =
            "Copies jar artifact and all files from directory that contains internal custom connector components to connector build directory."

        doLast {
            copyInternalComponents(defaultSrcDir, defaultArtifactName, defaultBuildDir)
        }
    }

    register("copySdkComponents") {
        group = "Snowflake"
        description = "Copies .sql files from $sdkComponentsDirName directory to the connector build file."

        doLast {
            copySdkComponents(libraryName, defaultBuildDir, sdkComponentsDirName)
        }
    }
}

/*
* **********************************************
*              TASK MAIN LOGIC
* **********************************************
*/

fun copyInternalComponents(defaultSrcDir: String, defaultArtifactName: String, defaultBuildDir: String) {
    TaskLogger.info("Starting 'copyInternalComponents' task...")
    val localSrcDir = getCommandArgument("srcDir") { defaultSrcDir }
    val artifact = getCommandArgument("artifact") { defaultArtifactName }
    val targetDir = getCommandArgument("targetDir") { defaultBuildDir }

    Utils.isDirectoryOrExit(localSrcDir)
    buildLocalJavaArtifact()
    copyLocalJavaArtifact(artifact, targetDir)

    copy {
        TaskLogger.info("Copying all files from local source directory [$defaultSrcDir] to connector build directory [$targetDir].")
        from(layout.projectDirectory.dir(localSrcDir.replace("./", "")))
        into(layout.projectDirectory.dir(targetDir.replace("./", "")))
    }
    TaskLogger.success("Local projects' jar artifact and all files from [$localSrcDir] copied to [$defaultBuildDir] directory.")
}

fun copySdkComponents(libraryName: String, defaultBuildDir: String, sdkComponentsDirName: String) {
    TaskLogger.info("Starting 'copySdkComponents' task...")
    val targetDir = getCommandArgument("targetDir") { defaultBuildDir }
    val jarPath = try {
        configurations.compileClasspath.get().find { it.name.startsWith(libraryName) }!!.path
    } catch (e: Exception) {
        Utils.exitWithErrorLog(
            "Unable to find [$libraryName] in the compile classpath. Make sure that the library is " +
                    "published to Maven local repository and the proper dependency is added to the build.gradle file."
        )
    }
    copy {
        TaskLogger.info("Copying [$sdkComponentsDirName] directory with .sql files to '$targetDir'")
        from(zipTree(jarPath))
        into(targetDir)
        include("$sdkComponentsDirName/**")
    }
    copy {
        TaskLogger.info("Copying [$libraryName] jar file to [$targetDir]")
        from(jarPath)
        into(targetDir)
        rename("^.*$libraryName.*\$", "$libraryName.jar")
    }
    TaskLogger.success("Copying sdk components finished successfully.")
}

/*
* **********************************************
*                 TASK UTILS
* **********************************************
*/

fun copyLocalJavaArtifact(artifact: String, targetDir: String) {
    project.copy {
        val originalArtifactName = "${project.name}-${project.version}.jar"
        TaskLogger.info("Copying jar artifact [$originalArtifactName] of local project to [${targetDir}] as [$artifact].")
        from(layout.projectDirectory.file("build/libs/${originalArtifactName}"))
        into(layout.projectDirectory.dir(targetDir.replace("./", "")))
        rename("^.*${project.name}-${project.version}.jar.*\$", artifact)
    }
}

fun buildLocalJavaArtifact() {
    TaskLogger.info("Building local jar artifact from local project.")
    val process = ProcessBuilder("./gradlew", "build").redirectErrorStream(true).start()
    Utils.executeCommand(
        process,
        "BUILD FAILED in"
    ) { Utils.exitWithErrorLog("Gradle build failed. Cannot create a jar artifact.") }
}

fun getCommandArgument(propertyName: String, defaultValue: () -> String): String {
    return project.findProperty(propertyName) as String? ?: defaultValue()
}

object TaskLogger {
    val redText = "\u001B[31m"
    val lightBlueText = "\u001B[96m"
    val greenText = "\u001B[92m"
    val blueText = "\u001B[36m"
    val yellowText = "\u001B[93m"
    val defaultText = "\u001B[0m"

    fun error(log: String) {
        println("${redText}[ERROR]: ${log}${defaultText}")
    }

    fun info(log: String) {
        println("${lightBlueText}[INFO]: ${log}${defaultText}")
    }

    fun success(log: String) {
        println("${greenText}[SUCCESS]: ${log}${defaultText}")
    }

    fun external(log: String) {
        println("${blueText}[EXTERNAL_LOG]: ${log}${defaultText}")
    }

    fun input(log: String) {
        println("${yellowText}[INPUT_REQUIRED]: ${log}${defaultText}")
    }
}

object Utils {
    fun isDirectoryOrExit(path: String) {
        val buildDir = File(path)
        if (!buildDir.isDirectory || !buildDir.exists()) {
            exitWithErrorLog("File [${buildDir}] does not exist or is not a directory.")
        }
    }

    fun exitWithErrorLog(log: String) {
        TaskLogger.error(log)
        TaskLogger.error("Task execution failed.")
        throw NativeSdkTaskException(log)
    }

    fun executeCommand(command: Process, errorLine: String, onErrorAction: Runnable): List<String> {
        val reader = BufferedReader(InputStreamReader(command.inputStream))
        val commandOutput = LinkedList<String>()
        reader.lines().forEach {
            TaskLogger.external(it)
            commandOutput.add(it)
            if (it.contains(errorLine)) {
                onErrorAction.run()
            }
        }
        return commandOutput
    }
}

class NativeSdkTaskException(message: String) : RuntimeException(message)
