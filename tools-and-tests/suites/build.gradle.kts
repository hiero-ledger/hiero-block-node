// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("application")
}

description = "Hiero Block Node E2E Suites"

application {
    mainModule = "org.hiero.block.suites"
    mainClass = "org.hiero.block.suites.BaseSuite"
}

mainModuleInfo {
    runtimeOnly("org.testcontainers.junit.jupiter")
    runtimeOnly("org.junit.jupiter.engine")
    runtimeOnly("org.junit.platform.launcher")
    runtimeOnly("org.testcontainers")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.common.media.type")
    runtimeOnly("io.helidon.common.tls")
    runtimeOnly("io.helidon.http")
    runtimeOnly("io.helidon.webclient.http2")
    runtimeOnly("org.hiero.block.protobuf.pbj")
}

tasks.register<Test>("runSuites") {
    description = "Runs E2E Test Suites"
    group = "suites"

    // @todo(#813) All of the docker processing belongs here, not in block-node-app.
    //    This might mean duplication, which is perfectly fine.
    dependsOn(":block-node-app:createDockerImageCI")

    useJUnitPlatform() { excludeTags("api") }
    testLogging { events("passed", "skipped", "failed") }
    testClassesDirs = sourceSets["main"].output.classesDirs
    classpath = sourceSets["main"].runtimeClasspath

    // Pass the block-node version as a system property
    systemProperty("block.node.version", project(":block-node-app").version.toString())
}

tasks.register<Test>("runAPISuites") {
    description = "Runs API E2E Test Suites"
    group = "api"
    mainModuleInfo {
        runtimeOnly("org.hiero.block.node.archive.s3cloud")
        runtimeOnly("org.hiero.block.node.stream.publisher")
        runtimeOnly("org.hiero.block.node.stream.subscriber")
        runtimeOnly("org.hiero.block.node.verification")
        runtimeOnly("org.hiero.block.node.blocks.files.historic")
        runtimeOnly("org.hiero.block.node.blocks.files.recent")
        runtimeOnly("org.hiero.block.node.backfill")
    }

    useJUnitPlatform() { includeTags("api") }
    testLogging { events("passed", "skipped", "failed") }
    testClassesDirs = sourceSets["main"].output.classesDirs
    classpath = sourceSets["main"].runtimeClasspath

    // Pass the block-node version as a system property
    systemProperty("block.node.version", project(":block-node-app").version.toString())
}
