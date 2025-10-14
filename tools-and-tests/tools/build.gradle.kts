// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.application")
    id("org.hiero.gradle.module.library")
    id("com.hedera.pbj.pbj-compiler")
    id("org.hiero.gradle.feature.legacy-classpath") // due to 'com.google.cloud.storage'
    id("org.hiero.gradle.feature.shadow")
}

description = "Hiero Block Stream Tools"

application { mainClass = "org.hiero.block.tools.BlockStreamTool" }

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.3")
}

tasks.test {
    useJUnitPlatform()
}

mainModuleInfo {
    requires("org.hiero.block.protobuf.pbj")
    requires("com.hedera.pbj.runtime")
    requires("com.github.luben.zstd_jni")
    requires("com.google.api.gax")
    requires("com.google.auth.oauth2")
    requires("com.google.cloud.core")
    requires("com.google.cloud.storage")
    requires("com.google.gson")
    requires("info.picocli")
    requires("org.apache.commons.compress")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.grpc.netty")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    requires("com.hedera.pbj.grpc.helidon")
    requires("com.hedera.pbj.runtime")
    requires("io.helidon.common")
    requires("io.helidon.webserver")
    requires("io.helidon.webserver.http2")
    requires("com.hedera.pbj.grpc.client.helidon")
    requires("io.helidon.common.tls")
    requires("io.helidon.webclient.api")
    requires("io.helidon.webclient.grpc")
    requires("io.helidon.webclient.http2")
    requires("org.antlr.antlr4.runtime")
}

testModuleInfo { requires("org.junit.jupiter.api") }

pbj { generateTestClasses = false }

sourceSets {
    main {
        pbj {
            srcDir(layout.projectDirectory.dir("src/main/java/org/hiero/block/tools/config/proto/"))
        }
    }
}

// Docker related tasks
// Vals
val dockerProjectRootDirectory: Directory = layout.projectDirectory.dir("docker")
val dockerBuildRootDirectory: Directory = layout.buildDirectory.dir("docker").get()

val copyDockerFolder: TaskProvider<Copy> =
    tasks.register<Copy>("copyDockerFolder") {
        description = "Copies the docker folder to the build root directory"
        group = "docker"

        from(dockerProjectRootDirectory)
        into(dockerBuildRootDirectory)
    }

val createDockerImage: TaskProvider<Exec> =
    tasks.register<Exec>("createDockerImage") {
        description = "Creates the docker image for the BlockStream Tools"
        group = "docker"

        dependsOn(copyDockerFolder, tasks.assemble)
        workingDir(dockerBuildRootDirectory)
        commandLine("sh", "-c", "./docker-build.sh ${project.version}")
    }

tasks.register<Exec>("startDockerContainerNetworkCapacityServer") {
    description =
        "Starts the docker BlockStream Tools with networkCapacity server mode and current defaults"
    group = "docker"

    dependsOn(createDockerImage)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version}  && docker compose -p block-tools-network-capacity up -d block-tool-server ",
    )
}

tasks.register<Exec>("startDockerContainerNetworkCapacityClient") {
    description =
        "Starts the docker BlockStream Tools with networkCapacity in client mode and current defaults"
    group = "docker"

    dependsOn(createDockerImage)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version}  && docker compose -p block-tools-network-capacity up -d block-tool-client ",
    )
}
