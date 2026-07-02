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
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.hiero.block.node.access.service")
    runtimeOnly("org.hiero.block.node.archive.s3cloud")
    runtimeOnly("org.hiero.block.node.backfill")
    runtimeOnly("org.hiero.block.node.blocks.files.historic")
    runtimeOnly("org.hiero.block.node.blocks.files.recent")
    runtimeOnly("org.hiero.block.node.cloud.storage.expanded")
    runtimeOnly("org.hiero.block.node.health")
    runtimeOnly("org.hiero.block.node.messaging")
    runtimeOnly("org.hiero.block.node.server.status")
    runtimeOnly("org.hiero.block.node.stream.publisher")
    runtimeOnly("org.hiero.block.node.stream.subscriber")
    runtimeOnly("org.hiero.block.node.tss.boostrap")
    runtimeOnly("org.hiero.block.node.verification")
    runtimeOnly("org.hiero.block.protobuf.pbj")
    runtimeOnly("io.helidon.common.media.type")
    runtimeOnly("io.helidon.common.tls")
    runtimeOnly("io.helidon.http")
    runtimeOnly("io.helidon.webclient.http2")
    runtimeOnly("org.junit.jupiter.engine")
    runtimeOnly("org.junit.platform.launcher")
    runtimeOnly("org.testcontainers")
    runtimeOnly("org.testcontainers.junit.jupiter")
    runtimeOnly("s3mock.testcontainers")
}

// =============================================================================
// E2E Test Plugin Configuration
// =============================================================================
// Plugins are built locally and mounted into the test container.
// Uses proper cross-project dependencies to resolve plugin and core jars.

val testPluginsDir: Provider<Directory> = layout.buildDirectory.dir("test-plugins")

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
    testPlugins(project(path = ":app", configuration = "blockNodePlugins"))
    implementation(project(":common"))
    implementation(project(":base"))
}

// Task to prepare plugins for E2E test container mounting
val prepareTestPlugins by
    tasks.registering {
        description = "Copies plugin jars for E2E test container mounting"
        group = "suites"

        val pluginFiles: FileCollection = testPlugins.incoming.files
        val coreFiles: FileCollection = appCoreRuntime.incoming.files
        val outputDir: Provider<Directory> = testPluginsDir

        inputs.files(pluginFiles)
        inputs.files(coreFiles)
        outputs.dir(outputDir)

        doLast {
            // Normalize jar names by stripping Gradle's "-module" suffix so that
            // e.g. "lazysodium-java-5.1.4-module.jar" (core) matches
            // "lazysodium-java-5.1.4.jar" (plugin transitive dep).
            // NOTE: This filtering logic is duplicated in block-node/app/build.gradle.kts
            // (prepareDockerPlugins task). Keep both in sync when changing.
            val normalizeJarName = { name: String -> name.replace("-module.jar", ".jar") }
            val coreJarNames: Set<String> =
                coreFiles.files.map { normalizeJarName(it.name) }.toSet()
            val targetDir: File = outputDir.get().asFile
            targetDir.deleteRecursively()
            targetDir.mkdirs()
            pluginFiles.files
                .filter { normalizeJarName(it.name) !in coreJarNames }
                .forEach { jar -> jar.copyTo(File(targetDir, jar.name), overwrite = true) }
        }
    }

// =============================================================================
// Block Pusher fat-jar
// =============================================================================
// Produces a self-contained, classpath-runnable jar (block-pusher.jar) bundling BlockNodeE2EClient
// and its
// runtime dependencies. Although the suites module is JPMS, a flattened fat-jar is launched from
// the
// classpath (java -jar), not the module path; BlockNodeE2EClient only uses APIs (PBJ gRPC client,
// Helidon
// WebClient, generated PBJ types) that work fine from the classpath.
// Resolve the runtime dependencies as PLAIN jars (javaModule=false) via an artifactView. The suites
// module is built on the module path, so `runtimeClasspath` carries the `javaModule=true` attribute
// and
// the gradlex ExtraJavaModuleInfoTransform tries to module-ify every artifact — including local
// project
// jars that are not yet built — which fails at configuration time with "File does not exist".
// Requesting javaModule=false skips that transform; `incoming.artifactView(...).files` is a lazy
// FileCollection that
// also wires the producing jar tasks as build dependencies. The flattened jar is launched from the
// classpath (`java -jar`), so module metadata is irrelevant at runtime.
val blockPusherRuntimeJars: FileCollection =
    configurations.runtimeClasspath
        .get()
        .incoming
        .artifactView {
            attributes {
                attribute(Attribute.of("javaModule", Boolean::class.javaObjectType), false)
            }
        }
        .files

tasks.register<Jar>("blockPusherJar") {
    description =
        "Builds a self-contained fat-jar that runs BlockNodeE2EClient against an external Block Node"
    group = "suites"

    dependsOn(tasks.named("compileJava"))

    archiveFileName.set("block-pusher.jar")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest { attributes("Main-Class" to "org.hiero.block.suites.e2e.BlockNodeE2EClient") }

    // The compiled classes of this module plus the full runtime classpath, flattened.
    from(sourceSets["main"].output)
    inputs.files(blockPusherRuntimeJars)
    from({ blockPusherRuntimeJars.filter { it.name.endsWith(".jar") }.map { zipTree(it) } })

    // Drop signature files from signed dependency jars so the flattened jar remains valid.
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.EC")
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
    systemProperty("plugins.dir", testPluginsDir.get().asFile.absolutePath)
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
    systemProperty("plugins.dir", testPluginsDir.get().asFile.absolutePath)
}
