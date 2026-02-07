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

// Authoritative list of block node plugins. When adding a new plugin, add it here
// and in testModuleInfo below. See docs/block-node/architecture/plugins.md for details.

val blockNodePlugins: Configuration by
    configurations.creating {
        // Other projects (e.g. suites) can depend on this configuration's artifacts
        isCanBeConsumed = true
        // This configuration can be resolved into actual jar files
        isCanBeResolved = true
        // Include transitive dependencies (e.g. gRPC, Helidon) alongside direct plugin jars
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
 * Configures a JavaExec task to run the block node with plugins. Plugin jars are added to the
 * classpath so Gradle's inferModulePath places them on the module path alongside core runtime jars.
 */
fun JavaExec.configureWithPlugins(pluginFiles: FileCollection, cleanStorage: Boolean = false) {
    group = "application"
    modularity.inferModulePath = true

    val serverDataDir = layout.buildDirectory.get().dir("block-node-storage")

    classpath = sourceSets["main"].runtimeClasspath + pluginFiles

    if (cleanStorage) {
        doFirst {
            val storageDir = serverDataDir.asFile
            if (storageDir.exists()) {
                storageDir.deleteRecursively()
            }
        }
    }

    configureBlockNodeEnvironment(serverDataDir)
}

// Configure the default 'run' task from the application plugin to use all plugins
tasks.named<JavaExec>("run") {
    description = "Run the block node with all plugins"
    configureWithPlugins(blockNodePlugins.incoming.files)
}

// Run with all plugins and clean storage
tasks.register<JavaExec>("runWithCleanStorage") {
    description = "Run the block node with all plugins, deleting storage first"
    mainClass = application.mainClass
    mainModule = application.mainModule
    configureWithPlugins(blockNodePlugins.incoming.files, cleanStorage = true)
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
            // Normalize jar names by stripping Gradle's "-module" suffix so that
            // e.g. "lazysodium-java-5.1.4-module.jar" (core) matches
            // "lazysodium-java-5.1.4.jar" (plugin transitive dep).
            // NOTE: This filtering logic is duplicated in tools-and-tests/suites/build.gradle.kts
            // (prepareTestPlugins task). Keep both in sync when changing.
            val normalizeJarName = { name: String -> name.replace("-module.jar", ".jar") }
            val coreJarNames: Set<String> =
                coreFiles.files.map { normalizeJarName(it.name) }.toSet()
            outputDir.deleteRecursively()
            outputDir.mkdirs()
            pluginFiles.files
                .filter { normalizeJarName(it.name) !in coreJarNames }
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
