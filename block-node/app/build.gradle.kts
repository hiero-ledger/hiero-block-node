// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("application")
}

description = "Hiero Block Node Server App"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

application {
    mainModule = "org.hiero.block.node.app"
    mainClass = "org.hiero.block.node.BlockNodeApp"
}

mainModuleInfo {
    annotationProcessor("dagger.compiler")
    annotationProcessor("com.google.auto.service.processor")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    annotationProcessor("dagger.compiler")
    requires("com.lmax.disruptor")
    requires("com.swirlds.common")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito")
    requires("org.mockito.junit.jupiter")
    requires("org.assertj.core")
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
