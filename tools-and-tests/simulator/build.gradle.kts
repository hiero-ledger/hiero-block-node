// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
    id("application")
}

description = "Hiero Block Stream Simulator"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

application {
    mainModule = "org.hiero.block.simulator"
    mainClass = "org.hiero.block.simulator.BlockStreamSimulator"
}

mainModuleInfo {
    annotationProcessor("dagger.compiler")
    annotationProcessor("com.google.auto.service.processor")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.grpc.netty")
}

testModuleInfo {
    requires("org.hiero.block.simulator.test.fixtures")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito")
    requires("org.mockito.junit.jupiter")
    requires("org.assertj.core")
    runtimeOnly("com.google.protobuf")
}

tasks.test {
    // for SimulatorMappedConfigSourceInitializerTest.extractConfigMappings()
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "--add-opens",
                "com.swirlds.config.extensions/com.swirlds.config.extensions.sources=org.hiero.block.simulator",
            )
        }
    )
}

// Task to run simulator in Publisher mode
tasks.register<JavaExec>("runPublisherClient") {
    description = "Run the simulator in Publisher Client mode"
    group = "application"

    mainClass = application.mainClass
    mainModule = application.mainModule
    classpath = sourceSets["main"].runtimeClasspath

    environment("BLOCK_STREAM_SIMULATOR_MODE", "PUBLISHER_CLIENT")
    environment("PROMETHEUS_ENDPOINT_ENABLED", "true")
    environment("PROMETHEUS_ENDPOINT_PORT_NUMBER", "16008")
}

tasks.register<JavaExec>("runPublisherServer") {
    description = "Run the simulator in Publisher Server mode"
    group = "application"

    mainClass = application.mainClass
    mainModule = application.mainModule
    classpath = sourceSets["main"].runtimeClasspath

    environment("BLOCK_STREAM_SIMULATOR_MODE", "PUBLISHER_SERVER")
    environment("PROMETHEUS_ENDPOINT_ENABLED", "true")
    environment("PROMETHEUS_ENDPOINT_PORT_NUMBER", "16010")
}

// Task to run simulator in Consumer mode
tasks.register<JavaExec>("runConsumer") {
    description = "Run the simulator in Consumer mode"
    group = "application"

    mainClass = application.mainClass
    mainModule = application.mainModule
    classpath = sourceSets["main"].runtimeClasspath

    environment("BLOCK_STREAM_SIMULATOR_MODE", "CONSUMER")
    environment("PROMETHEUS_ENDPOINT_ENABLED", "true")
    environment("PROMETHEUS_ENDPOINT_PORT_NUMBER", "16009")
}

// Vals
val dockerProjectRootDirectory: Directory = layout.projectDirectory.dir("docker")
var resourcesProjectRootDirectory: Directory = layout.projectDirectory.dir("src/main/resources")
var distributionBuildRootDirectory: Directory = layout.buildDirectory.dir("distributions").get()
val dockerBuildRootDirectory: Directory = layout.buildDirectory.dir("docker").get()

// Docker related tasks
val copyDockerFolder: TaskProvider<Copy> =
    tasks.register<Copy>("copyDockerFolder") {
        description = "Copies the docker folder to the build root directory"
        group = "docker"

        from(dockerProjectRootDirectory)
        into(dockerBuildRootDirectory)
    }

// Docker related tasks
val copyDependenciesFolders: TaskProvider<Copy> =
    tasks.register<Copy>("copyDependenciesFolders") {
        description = "Copies the docker folder to the build root directory"
        group = "docker"

        dependsOn(copyDockerFolder, tasks.assemble)
        from(resourcesProjectRootDirectory)
        from(distributionBuildRootDirectory)
        into(dockerBuildRootDirectory)
    }

val createDockerImage: TaskProvider<Exec> =
    tasks.register<Exec>("createDockerImage") {
        description = "Creates the docker image of the Block Stream Simulator"
        group = "docker"

        dependsOn(copyDependenciesFolders, tasks.assemble)
        workingDir(dockerBuildRootDirectory)
        commandLine("sh", "-c", "docker buildx build -t hedera-block-simulator:latest .")
    }

tasks.register<Exec>("startDockerContainer") {
    description = "Creates and starts the docker image of the Block Stream Simulator"
    group = "docker"
    dependsOn(createDockerImage, tasks.assemble)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh && docker compose -f docker-compose-publisher-consumer.yml -p simulator up -d",
    )
}

tasks.register<Exec>("startDockerContainerPublisher") {
    description = "Creates and starts the docker image of the Block Stream Simulator Publisher"
    group = "docker"
    dependsOn(createDockerImage, tasks.assemble)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh && docker compose -f docker-compose-publisher.yml -p simulator up -d",
    )
}

tasks.register<Exec>("startDockerContainerPublisherDebug") {
    description =
        "Creates and starts with debug the docker image of the Block Stream Simulator Publisher"
    group = "docker"
    dependsOn(createDockerImage, tasks.assemble)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh true && docker compose -f docker-compose-publisher.yml -p simulator up -d",
    )
}

tasks.register<Exec>("stopDockerContainer") {
    description = "Stops running docker containers of the Block Stream Simulator"
    group = "docker"
    dependsOn(copyDockerFolder)
    workingDir(dockerBuildRootDirectory)
    commandLine("sh", "-c", "docker compose -p simulator stop")
}
