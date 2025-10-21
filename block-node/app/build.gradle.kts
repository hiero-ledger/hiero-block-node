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

tasks.withType<JavaExec>().configureEach {
    modularity.inferModulePath = true
    val serverDataDir = layout.buildDirectory.get().dir("block-node-storage")
    environment("FILES_HISTORIC_ROOT_PATH", "${serverDataDir}/files-historic")
    environment("FILES_RECENT_LIVE_ROOT_PATH", "${serverDataDir}/files-live")
    environment("FILES_RECENT_UNVERIFIED_ROOT_PATH", "${serverDataDir}/files-unverified")
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
}

application {
    mainModule = "org.hiero.block.node.app"
    mainClass = "org.hiero.block.node.app.BlockNodeApp"
}

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    // List of all "plugin modules" we might someday need at runtime.
    // In the future, we may get Gradle to automatically infer this block
    //   https://github.com/gradlex-org/java-module-dependencies/issues/174
    runtimeOnly("org.hiero.block.node.archive.s3cloud")
    runtimeOnly("org.hiero.block.node.messaging")
    runtimeOnly("org.hiero.block.node.health")
    runtimeOnly("org.hiero.block.node.stream.publisher")
    runtimeOnly("org.hiero.block.node.stream.subscriber")
    runtimeOnly("org.hiero.block.node.verification")
    runtimeOnly("org.hiero.block.node.blocks.files.historic")
    runtimeOnly("org.hiero.block.node.blocks.files.recent")
    runtimeOnly("org.hiero.block.node.access.service")
    runtimeOnly("org.hiero.block.node.server.status")
    runtimeOnly("org.hiero.block.node.backfill")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")

    requires("io.grpc")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito")
    requires("org.assertj.core")
    requires("io.helidon.http.media")
    requires("io.helidon.webclient.http2")
    requires("io.grpc.netty")
    requires("com.google.protobuf.util")
    requires("com.hedera.pbj.grpc.helidon.config")
    requires("com.hedera.pbj.grpc.client.helidon")
    requires("com.hedera.pbj.grpc.helidon")
    requires("io.helidon.webclient.grpc")

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

val createDockerImage: TaskProvider<Exec> =
    tasks.register<Exec>("createDockerImage") {
        description =
            "Creates the production docker image of the Block Node Server based on the current version"
        group = "docker"

        dependsOn(copyDockerFolder, tasks.assemble)
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

tasks.register<Exec>("startDockerContainerSmokeTest") {
    description =
        "Starts the docker smoke test container of the Block Node Server for the current version"
    group = "docker"

    dependsOn(createDockerImage)
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
