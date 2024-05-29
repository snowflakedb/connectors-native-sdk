/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.examples.example_github_connector

class BuildingHelper {
    static int runCommand(String command, File workingDir) {
        def process = new ProcessBuilder(command.split(" ")).directory(workingDir).start()
        def reader = new BufferedReader(new InputStreamReader(process.getInputStream()))

        String line
        while ((line = reader.readLine()) != null) {
            println line
        }

        int exitStatus = process.waitFor()
        return exitStatus
    }
}
