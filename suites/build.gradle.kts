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
}

tasks.register<Test>("runSuites") {
    description = "Runs E2E Test Suites"
    group = "suites"

    // @todo(#343) - :server:createProductionDotEnv should disappear
    // @todo(#813) All of the docker processing belongs here, not in block-node-app.
    //    This might mean duplication, which is perfectly fine.
    dependsOn(":block-node-app:createDockerImage", ":block-node-app:createProductionDotEnv")

    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }
    testClassesDirs = sourceSets["main"].output.classesDirs
    classpath = sourceSets["main"].runtimeClasspath

    // Pass the block-node version as a system property
    systemProperty("block.node.version", project(":block-node-app").version.toString())
}
