// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
    id("application")
}

description = "Hiero Block Node Server App"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

// Adjust the generated start scripts to use the 'lib' and 'plugins' folder for the module path.
// The OCI image is built with only core dependencies. Plugins are downloaded at deployment time
// into the 'plugins' folder, allowing dynamic selection of which plugins to load.
tasks.startScripts {
    classpath = files()
    doLast {
        unixScript.writeText(
            unixScript
                .readText()
                .replace("MODULE_PATH=\n", "MODULE_PATH=\$APP_HOME/lib/:\$APP_HOME/plugins/\n")
        )
    }
}

distributions {
    main {
        contents {
            val pluginsDir = layout.buildDirectory.dir("tmp/plugins")

            from(pluginsDir) { into("plugins") }
        }
    }
}

tasks.distTar {
    val pluginsDir = layout.buildDirectory.dir("tmp/plugins")

    // Create an empty plugins directory with a .keep file so it exists in the distribution
    doFirst {
        val dir = pluginsDir.get().asFile
        dir.mkdirs()
        File(dir, ".keep").writeText("")
    }
}

tasks.withType<JavaExec>().configureEach {
    modularity.inferModulePath = true
    val serverDataDir = layout.buildDirectory.get().dir("block-node-storage")
    environment("FILES_HISTORIC_ROOT_PATH", "${serverDataDir}/files-historic")
    environment("FILES_RECENT_LIVE_ROOT_PATH", "${serverDataDir}/files-live")
    environment("FILES_RECENT_UNVERIFIED_ROOT_PATH", "${serverDataDir}/files-unverified")
    environment(
        "VERIFICATION_ALL_BLOCKS_HASHER_FILE_PATH",
        "${serverDataDir}/verification/rootHashOfAllPreviousBlocks.bin",
    )
    mainModuleInfo {
        runtimeOnly("org.hiero.block.node.messaging")
        runtimeOnly("org.hiero.block.node.health")
        runtimeOnly("org.hiero.block.node.access.service")
        runtimeOnly("org.hiero.block.node.server.status")
        runtimeOnly("org.hiero.block.node.archive.s3cloud")
        runtimeOnly("org.hiero.block.node.stream.publisher")
        runtimeOnly("org.hiero.block.node.stream.subscriber")
        runtimeOnly("org.hiero.block.node.verification")
        runtimeOnly("org.hiero.block.node.blocks.files.historic")
        runtimeOnly("org.hiero.block.node.blocks.files.recent")
        runtimeOnly("org.hiero.block.node.backfill")
    }
}

tasks.register<JavaExec>("runWithCleanStorage") {
    description = "Run the block node, deleting storage first"
    group = "application"

    mainClass = application.mainClass
    mainModule = application.mainModule
    classpath = sourceSets["main"].runtimeClasspath

    val serverDataDir = layout.buildDirectory.get().dir("block-node-storage")
    // delete the storage directory before starting the application
    doFirst {
        val storageDir = serverDataDir.asFile
        if (storageDir.exists()) {
            storageDir.deleteRecursively()
        }
    }
    environment("FILES_HISTORIC_ROOT_PATH", "${serverDataDir}/files-historic")
    environment("FILES_RECENT_LIVE_ROOT_PATH", "${serverDataDir}/files-live")
    environment("FILES_RECENT_UNVERIFIED_ROOT_PATH", "${serverDataDir}/files-unverified")

    environment(
        "VERIFICATION_ALL_BLOCKS_HASHER_FILE_PATH",
        "${serverDataDir}/verification/rootHashOfAllPreviousBlocks.bin",
    )
}

application {
    mainModule = "org.hiero.block.node.app"
    mainClass = "org.hiero.block.node.app.BlockNodeApp"
}

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")

    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito")
    requires("org.assertj.core")

    exportsTo("com.swirlds.config.impl")
}

// Vals
val dockerProjectRootDirectory: Directory = layout.projectDirectory.dir("docker")
val dockerBuildRootDirectory: Directory = layout.buildDirectory.dir("docker").get()

// Docker related tasks
val copyDockerFolder: TaskProvider<Copy> =
    tasks.register<Copy>("copyDockerFolder") {
        description = "Copies the docker folder to the build root directory"
        group = "docker"

        from(dockerProjectRootDirectory)
        into(dockerBuildRootDirectory)
    }

val copyDockerFolderCI: TaskProvider<Copy> =
    tasks.register<Copy>("copyDockerFolderCI") {
        description =
            "Copies the docker folder to the build root directory with CI specific files. To be used only in CI environments, for instance when running E2E tests."
        group = "docker"

        dependsOn(copyDockerFolder)
        from(dockerBuildRootDirectory.file("ci-logging.properties"))
        into(dockerBuildRootDirectory)
        rename { f -> "logging.properties" }
    }

val createDockerImage: TaskProvider<Exec> =
    tasks.register<Exec>("createDockerImage") {
        description =
            "Creates the production docker image of the Block Node Server based on the current version"
        group = "docker"

        dependsOn(copyDockerFolder, tasks.assemble)
        workingDir(dockerBuildRootDirectory)
        commandLine("sh", "-c", "./docker-build.sh ${project.version}")
    }

val createDockerImageCI: TaskProvider<Exec> =
    tasks.register<Exec>("createDockerImageCI") {
        description =
            "Creates the production docker image of the Block Node Server based on the current version, but with CI optimizations. Intended only for use in CI environments, like running E2E tests!"
        group = "docker"

        dependsOn(copyDockerFolderCI, tasks.assemble)
        workingDir(dockerBuildRootDirectory)
        commandLine("sh", "-c", "./docker-build.sh ${project.version}")
    }

tasks.register<Exec>("startDockerContainer") {
    description =
        "Starts the docker production container of the Block Node Server for the current version"
    group = "docker"

    dependsOn(createDockerImage)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version} false false && docker compose -p block-node up -d",
    )
}

tasks.register<Exec>("startDockerContainerDebug") {
    description =
        "Starts the docker debug container of the Block Node Server for the current version"
    group = "docker"

    dependsOn(createDockerImage)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version} true false && docker compose -p block-node up -d",
    )
}

tasks.register<Exec>("startDockerContainerCI") {
    description =
        "Starts the docker CI test container of the Block Node Server for the current version"
    group = "docker"

    dependsOn(createDockerImageCI)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version} false true && docker compose -p block-node up -d",
    )
}

tasks.register<Exec>("stopDockerContainer") {
    description = "Stops running docker containers of the Block Node Server"
    group = "docker"

    dependsOn(copyDockerFolder)
    workingDir(dockerBuildRootDirectory)
    commandLine("sh", "-c", "docker compose -p block-node stop")
}
