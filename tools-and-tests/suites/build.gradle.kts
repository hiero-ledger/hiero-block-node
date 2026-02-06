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
// Uses proper cross-project dependencies to resolve plugin and core jars.

// Resolves the app's core runtime jars (for exclusion filtering)
val appCoreRuntime: Configuration by
    configurations.creating {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = true
    }

// Resolves all plugin jars from the app's allPlugins configuration
val testPlugins: Configuration by
    configurations.creating {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = true
    }

dependencies {
    // Both configurations need version constraints for transitive dependency resolution
    appCoreRuntime(platform(project(":hiero-dependency-versions")))
    appCoreRuntime(project(":app"))
    testPlugins(project(path = ":app", configuration = "allPlugins"))
}

// Task to prepare plugins for E2E test container mounting
val prepareTestPlugins by
    tasks.registering {
        description = "Copies plugin jars for E2E test container mounting"
        group = "suites"

        val pluginFiles: FileCollection = testPlugins.incoming.files
        val coreFiles: FileCollection = appCoreRuntime.incoming.files
        val outputDir: Provider<Directory> = layout.buildDirectory.dir("test-plugins")

        inputs.files(pluginFiles)
        inputs.files(coreFiles)
        outputs.dir(outputDir)

        doLast {
            val coreJarNames: Set<String> = coreFiles.files.map { it.name }.toSet()
            val targetDir: File = outputDir.get().asFile
            targetDir.deleteRecursively()
            targetDir.mkdirs()
            pluginFiles.files
                .filter { it.name !in coreJarNames }
                .forEach { jar -> jar.copyTo(File(targetDir, jar.name), overwrite = true) }
        }
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
