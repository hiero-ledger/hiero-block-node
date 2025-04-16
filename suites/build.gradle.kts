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

dependencies {
    implementation(platform("org.junit:junit-bom:5.12.2"))

    implementation("org.testcontainers:testcontainers")
    runtimeOnly("org.testcontainers:junit-jupiter")
    runtimeOnly("org.junit.jupiter:junit-jupiter")
    runtimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.register<Test>("runSuites") {
    description = "Runs E2E Test Suites"
    group = "suites"
    modularity.inferModulePath = false

    // @todo(#343) - :server:createProductionDotEnv should disappear
    // @todo(#813) All of the docker processing belongs here, not in block-node-server.
    //    This might mean duplication, which is perfectly fine.
    dependsOn(":block-node-server:createDockerImage", ":block-node-server:createProductionDotEnv")

    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }
    testClassesDirs = sourceSets["main"].output.classesDirs
    classpath = sourceSets["main"].runtimeClasspath
}
