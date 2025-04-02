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

    modularity.inferModulePath = true
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
    // List of all "plugin modules" we need at runtime.
    // In the future, we may get Gradle to automatically infer this block
    //   https://github.com/gradlex-org/java-module-dependencies/issues/174
    runtimeOnly("org.hiero.block.node.messaging")
    runtimeOnly("org.hiero.block.node.health")
    runtimeOnly("org.hiero.block.node.publisher")
    runtimeOnly("org.hiero.block.node.subscriber")
    runtimeOnly("org.hiero.block.node.verification")
    runtimeOnly("org.hiero.block.node.blocks.cloud.archive")
    runtimeOnly("org.hiero.block.node.blocks.cloud.historic")
    runtimeOnly("org.hiero.block.node.blocks.files.historic")
    runtimeOnly("org.hiero.block.node.blocks.files.recent")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
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

// @todo(#343) - createProductionDotEnv is temporary and used by the suites,
//  once the suites no longer rely on the .env file from the build root we
//  should remove this task
val createProductionDotEnv: TaskProvider<Exec> =
    tasks.register<Exec>("createProductionDotEnv") {
        description = "Creates the default dotenv file for the Block Node Server"
        group = "docker"

        dependsOn(copyDockerFolder)
        workingDir(dockerBuildRootDirectory)
        commandLine("sh", "-c", "./update-env.sh ${project.version} false false")
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
