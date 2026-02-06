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

// Adjust the generated start scripts to use the 'lib' and 'plugins' folder for the module path.
// The OCI image is built with only core dependencies. Plugins are downloaded at deployment time
// into the 'plugins' folder, allowing dynamic selection of which plugins to load.
tasks.startScripts {
    classpath = files()
    doLast {
        unixScript.writeText(
            unixScript
                .readText()
                .replace("MODULE_PATH=\n", "MODULE_PATH=\$APP_HOME/lib/:\$APP_HOME/plugins/\n")
        )
    }
}

distributions {
    main {
        contents {
            val pluginsDir = layout.buildDirectory.dir("tmp/plugins")

            from(pluginsDir) { into("plugins") }
        }
    }
}

tasks.distTar {
    val pluginsDir = layout.buildDirectory.dir("tmp/plugins")

    // Create an empty plugins directory with a .keep file so it exists in the distribution
    doFirst {
        val dir = pluginsDir.get().asFile
        dir.mkdirs()
        File(dir, ".keep").writeText("")
    }
}

application {
    mainModule = "org.hiero.block.node.app"
    mainClass = "org.hiero.block.node.app.BlockNodeApp"
}

// Core runtime dependencies only - NO plugins
// Plugins are loaded dynamically from the plugins directory at runtime
mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

// =============================================================================
// Plugin Configuration for Dynamic Loading
// =============================================================================
// Plugins are NOT bundled in the OCI image - they are loaded at runtime from the
// plugins directory, which can be populated via Helm chart, docker mount, or Gradle.
// Profile-specific plugin selection (minimal, lfh, rfh) is handled by Helm chart
// values-overrides and the prepare-plugins.sh script for local docker-compose.
//
// This is the authoritative list of block node plugins. When adding a new plugin,
// add it here and in testModuleInfo below. Transitive dependencies (e.g. gRPC,
// Helidon, Swirlds libraries) are resolved automatically and filtered against the
// core runtime to avoid duplicates.

val blockNodePlugins: Configuration by
    configurations.creating {
        isCanBeConsumed = true
        isCanBeResolved = true
        isTransitive = true
    }

dependencies {
    // Version constraints for transitive dependency resolution
    blockNodePlugins(platform(project(":hiero-dependency-versions")))

    // Facilities
    blockNodePlugins(project(":facility-messaging"))

    // Services
    blockNodePlugins(project(":health"))
    blockNodePlugins(project(":server-status"))
    blockNodePlugins(project(":block-access-service"))
    blockNodePlugins(project(":stream-publisher"))
    blockNodePlugins(project(":stream-subscriber"))
    blockNodePlugins(project(":verification"))

    // Storage
    blockNodePlugins(project(":blocks-file-recent"))
    blockNodePlugins(project(":blocks-file-historic"))

    // Extended functionality
    blockNodePlugins(project(":backfill"))
    blockNodePlugins(project(":s3-archive"))
}

// =============================================================================
// Run Tasks with Plugin Profiles
// =============================================================================
// These tasks run the block node with different plugin configurations.
// Plugins are copied to a build directory and added to the module path.

/** Sets block node storage environment variables for the given data directory. */
fun JavaExec.configureBlockNodeEnvironment(serverDataDir: Directory) {
    environment("FILES_HISTORIC_ROOT_PATH", "${serverDataDir}/files-historic")
    environment("FILES_RECENT_LIVE_ROOT_PATH", "${serverDataDir}/files-live")
    environment("FILES_RECENT_UNVERIFIED_ROOT_PATH", "${serverDataDir}/files-unverified")
    environment(
        "VERIFICATION_ALL_BLOCKS_HASHER_FILE_PATH",
        "${serverDataDir}/verification/rootHashOfAllPreviousBlocks.bin",
    )
}

/**
 * Configures a JavaExec task to copy plugin jars (excluding core runtime jars) to a build directory
 * and add that directory to the module path. Uses FileCollection parameters instead of
 * Configuration to be configuration-cache compatible.
 */
fun JavaExec.configureWithPlugins(
    pluginFiles: FileCollection,
    coreFiles: FileCollection,
    pluginsDirName: String,
    cleanStorage: Boolean = false,
) {
    group = "application"
    modularity.inferModulePath = true
    classpath = sourceSets["main"].runtimeClasspath

    val serverDataDir = layout.buildDirectory.get().dir("block-node-storage")
    val pluginsDir = layout.buildDirectory.dir("run-plugins/$pluginsDirName")

    doFirst {
        if (cleanStorage) {
            val storageDir = serverDataDir.asFile
            if (storageDir.exists()) {
                storageDir.deleteRecursively()
            }
        }

        // Copy plugin jars (excluding jars already in core runtime) to plugins directory
        val coreJarNames: Set<String> = coreFiles.files.map { it.name }.toSet()
        val targetDir = pluginsDir.get().asFile
        targetDir.deleteRecursively()
        targetDir.mkdirs()

        pluginFiles.files
            .filter { it.name !in coreJarNames }
            .forEach { jar -> jar.copyTo(File(targetDir, jar.name), overwrite = true) }
    }

    // Add plugins directory to module path via JVM args
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf("--module-path", pluginsDir.get().asFile.absolutePath)
        }
    )

    configureBlockNodeEnvironment(serverDataDir)
}

// Configure the default 'run' task from the application plugin to use all plugins
tasks.named<JavaExec>("run") {
    description = "Run the block node with all plugins"
    configureWithPlugins(
        blockNodePlugins.incoming.files,
        sourceSets["main"].runtimeClasspath,
        "run",
    )
}

// Run with all plugins and clean storage
tasks.register<JavaExec>("runWithCleanStorage") {
    description = "Run the block node with all plugins, deleting storage first"
    mainClass = application.mainClass
    mainModule = application.mainModule
    configureWithPlugins(
        blockNodePlugins.incoming.files,
        sourceSets["main"].runtimeClasspath,
        "runWithCleanStorage",
        cleanStorage = true,
    )
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")

    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito")
    requires("org.assertj.core")

    // Plugins needed for integration tests (e.g., testMain which starts the full app)
    runtimeOnly("org.hiero.block.node.messaging")
    runtimeOnly("org.hiero.block.node.health")
    runtimeOnly("org.hiero.block.node.server.status")
    runtimeOnly("org.hiero.block.node.stream.publisher")
    runtimeOnly("org.hiero.block.node.stream.subscriber")
    runtimeOnly("org.hiero.block.node.verification")
    runtimeOnly("org.hiero.block.node.blocks.files.recent")
    runtimeOnly("org.hiero.block.node.blocks.files.historic")
    runtimeOnly("org.hiero.block.node.access.service")
    runtimeOnly("org.hiero.block.node.backfill")
    runtimeOnly("org.hiero.block.node.archive.s3cloud")

    exportsTo("com.swirlds.config.impl")
}

// =============================================================================
// Docker Tasks
// =============================================================================
// Single barebone Docker image - plugins are mounted or downloaded at deployment time.

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
        description = "Creates the barebone Docker image of the Block Node Server (no plugins)"
        group = "docker"

        dependsOn(copyDockerFolder, tasks.assemble)
        workingDir(dockerBuildRootDirectory)
        commandLine("sh", "-c", "./docker-build.sh ${project.version}")
    }

// Task to prepare plugins for local docker-compose deployment
val prepareDockerPlugins =
    tasks.register("prepareDockerPlugins") {
        description = "Copies all plugin jars to the docker plugins directory for local development"
        group = "docker"

        val pluginFiles: FileCollection = blockNodePlugins.incoming.files
        val coreFiles: FileCollection = sourceSets["main"].runtimeClasspath
        val outputDir: File = dockerBuildRootDirectory.dir("plugins").asFile

        inputs.files(pluginFiles)
        inputs.files(coreFiles)
        outputs.dir(outputDir)

        dependsOn(copyDockerFolder)

        doLast {
            val coreJarNames: Set<String> = coreFiles.files.map { it.name }.toSet()
            outputDir.deleteRecursively()
            outputDir.mkdirs()
            pluginFiles.files
                .filter { it.name !in coreJarNames }
                .forEach { jar -> jar.copyTo(File(outputDir, jar.name), overwrite = true) }
        }
    }

tasks.register<Exec>("startDockerContainer") {
    description = "Starts the docker container of the Block Node Server for the current version"
    group = "docker"

    dependsOn(createDockerImage, prepareDockerPlugins)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version} false false && docker compose -p block-node up -d",
    )
}

tasks.register<Exec>("startDockerContainerDebug") {
    description = "Starts the docker container with debug enabled"
    group = "docker"

    dependsOn(createDockerImage, prepareDockerPlugins)
    workingDir(dockerBuildRootDirectory)
    commandLine(
        "sh",
        "-c",
        "./update-env.sh ${project.version} true false && docker compose -p block-node up -d",
    )
}

tasks.register<Exec>("stopDockerContainer") {
    description = "Stops running docker containers of the Block Node Server"
    group = "docker"

    dependsOn(copyDockerFolder)
    workingDir(dockerBuildRootDirectory)
    commandLine("sh", "-c", "docker compose -p block-node stop")
}
