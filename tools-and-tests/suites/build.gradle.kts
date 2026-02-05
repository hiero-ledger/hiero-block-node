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

// =============================================================================
// E2E Test Plugin Configuration
// =============================================================================
// Plugins are built locally and mounted into the test container.
// This avoids external dependencies during E2E testing.

val testPlugins: Configuration by
    configurations.creating {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = true
    }

dependencies {
    // All plugins needed for E2E testing
    testPlugins(project(":facility-messaging"))
    testPlugins(project(":health"))
    testPlugins(project(":server-status"))
    testPlugins(project(":block-access-service"))
    testPlugins(project(":stream-publisher"))
    testPlugins(project(":stream-subscriber"))
    testPlugins(project(":verification"))
    testPlugins(project(":blocks-file-recent"))
    testPlugins(project(":blocks-file-historic"))
    testPlugins(project(":backfill"))
    testPlugins(project(":s3-archive"))
}

// Collect core jar names from the app's runtime classpath to exclude when copying plugins
val appCoreJarNames: Set<String> by lazy {
    project(":app").configurations.getByName("runtimeClasspath").files.map { it.name }.toSet()
}

// Task to prepare plugins for E2E test container mounting
val prepareTestPlugins by
    tasks.registering(Copy::class) {
        description = "Copies plugin jars for E2E test container mounting"
        group = "suites"

        from(testPlugins) {
            // Exclude jars that are already in the core image
            exclude { it.name in appCoreJarNames }
        }
        into(layout.buildDirectory.dir("test-plugins"))
    }

tasks.register<Test>("runSuites") {
    description = "Runs E2E Test Suites"
    group = "suites"

    // Build the barebone docker image and prepare plugins
    dependsOn(":app:createDockerImage", prepareTestPlugins)

    useJUnitPlatform() { excludeTags("api") }
    testLogging { events("passed", "skipped", "failed") }
    testClassesDirs = sourceSets["main"].output.classesDirs
    classpath = sourceSets["main"].runtimeClasspath

    // Pass block-node version and plugins directory to tests
    systemProperty("block.node.version", project(":app").version.toString())
    systemProperty(
        "plugins.dir",
        layout.buildDirectory.dir("test-plugins").get().asFile.absolutePath,
    )
}

tasks.register<Test>("runAPISuites") {
    description = "Runs API E2E Test Suites"
    group = "api"

    // Build the barebone docker image and prepare plugins
    dependsOn(":app:createDockerImage", prepareTestPlugins)

    useJUnitPlatform() { includeTags("api") }
    testLogging { events("passed", "skipped", "failed") }
    testClassesDirs = sourceSets["main"].output.classesDirs
    classpath = sourceSets["main"].runtimeClasspath

    // Pass block-node version and plugins directory to tests
    systemProperty("block.node.version", project(":app").version.toString())
    systemProperty(
        "plugins.dir",
        layout.buildDirectory.dir("test-plugins").get().asFile.absolutePath,
    )
}
