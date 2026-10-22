// SPDX-License-Identifier: Apache-2.0
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.security.DigestInputStream
import java.security.MessageDigest
import java.util.HexFormat
import javax.inject.Inject
import net.swiftzer.semver.SemVer
import org.gradle.process.ExecOperations

tasks.versionAsSpecified {
    val chartFiles =
        fileTree(rootDir) {
            include("charts/**/Chart.yaml")
            exclude("**/node_modules/")
        }

    val readmeFiles =
        fileTree(rootDir) {
            include("charts/**/README.md")
            exclude("**/node_modules/")
        }

    doLast {
        val newVersion = SemVer.parse(inputs.properties["newVersion"] as String).toString()

        // Update Chart.yaml files
        chartFiles.forEach { file ->
            val yaml = file.readText()
            val oldVersion = Regex("(?<=^(appVersion|version): ).+", RegexOption.MULTILINE)
            file.writeText(yaml.replace(oldVersion, newVersion))
        }

        // Update README.md files
        readmeFiles.forEach { file ->
            val readme = file.readText()
            val versionRegex = Regex("""(?<=export VERSION=").+?(?=")""")
            if (versionRegex.containsMatchIn(readme)) {
                file.writeText(readme.replace(versionRegex, newVersion))
            }
        }
    }
}

/**
 * Downloads test block archives (OCI images pinned in gradle.properties) into
 * .test-blocks/<archive>/. An archive is re-downloaded only when its pin changes; a directory with
 * unpublished changes is never replaced.
 */
abstract class FetchTestBlocks @Inject constructor(private val exec: ExecOperations) :
    DefaultTask() {
    @get:Input abstract val archives: SetProperty<String>
    @get:Input abstract val registry: Property<String>
    @get:Input abstract val pins: MapProperty<String, String>
    @get:Internal abstract val parentDir: DirectoryProperty
    @get:Input abstract val overridden: Property<Boolean>

    @TaskAction
    fun fetch() {
        val parent = parentDir.get().asFile
        for (archive in archives.get()) {
            if (!archive.matches(Regex("[a-z0-9-]+"))) {
                throw GradleException(
                    "Invalid test blocks archive name '$archive', expected [a-z0-9-]+"
                )
            }
            val pin =
                pins.get()[archive]
                    ?: throw GradleException(
                        "No pin for test blocks archive '$archive': add hiero.testBlocks.image.$archive to gradle.properties"
                    )
            val dir = File(parent, archive)
            if (overridden.get()) {
                if (!dir.isDirectory) {
                    throw GradleException("Test blocks directory $dir does not exist")
                }
            } else {
                fetchArchive(archive, "${registry.get()}/$pin", parent, dir)
            }
        }
    }

    private fun fetchArchive(archive: String, ref: String, parent: File, dir: File) {
        val stamp = File(dir, ".image-ref")
        if (!stamp.isFile || stamp.readText().trim() != ref) {
            if (dir.exists()) {
                val changes = diffAgainstManifest(dir)
                if (changes.isNotEmpty()) {
                    throw GradleException(
                        "$archive has unpublished changes; publish them as a new image version " +
                            "or delete $dir\n" +
                            changes.joinToString("\n")
                    )
                }
            }
            logger.lifecycle("Fetching test blocks $ref")
            // extract next to the target first, so a failed download never leaves a half-filled
            // archive
            val tmp = File(parent, ".$archive.tmp")
            tmp.deleteRecursively()
            tmp.mkdirs()
            exec.exec { commandLine("docker", "pull", "--quiet", ref) }
            val out = ByteArrayOutputStream()
            exec.exec {
                commandLine("docker", "create", ref, "/none")
                standardOutput = out
            }
            val container = out.toString().trim()
            try {
                exec.exec { commandLine("docker", "cp", "$container:/$archive", tmp.absolutePath) }
            } finally {
                // best-effort cleanup: a failure here must not hide a failed docker cp
                exec.exec {
                    commandLine("docker", "rm", container)
                    isIgnoreExitValue = true
                }
                // keep Docker from holding extra copies of (possibly multi-GB) archives
                exec.exec {
                    commandLine("docker", "rmi", ref)
                    isIgnoreExitValue = true
                }
            }
            val extracted = File(tmp, archive)
            val problems = diffAgainstManifest(extracted)
            if (problems.isNotEmpty()) {
                throw GradleException(
                    "$ref does not match its MANIFEST.sha256\n" + problems.joinToString("\n")
                )
            }
            File(extracted, ".image-ref").writeText(ref + "\n")
            dir.deleteRecursively()
            if (!extracted.renameTo(dir)) {
                throw GradleException("Could not move $extracted to $dir")
            }
            tmp.deleteRecursively()
        }
    }

    /** Files added, removed or changed compared to MANIFEST.sha256 (all files if there is none). */
    private fun diffAgainstManifest(dir: File): List<String> {
        val manifest = File(dir, "MANIFEST.sha256")
        val expected = mutableMapOf<String, String>()
        if (manifest.isFile) {
            for (line in manifest.readLines()) {
                if (line.isNotBlank()) {
                    // sha256sum format: "<hash>  ./<path>"
                    expected[line.substring(66).removePrefix("./")] = line.substring(0, 64)
                }
            }
        }
        val actual =
            dir.walk()
                .filter { it.isFile && it.name != "MANIFEST.sha256" && it.name != ".image-ref" }
                .associate { it.relativeTo(dir).invariantSeparatorsPath to sha256(it) }
        val changes = mutableListOf<String>()
        if (!manifest.isFile) {
            changes += "missing MANIFEST.sha256"
        }
        for ((path, hash) in actual) {
            val want = expected[path]
            if (want == null) {
                changes += "added:   $path"
            } else if (want != hash) {
                changes += "changed: $path"
            }
        }
        for (path in expected.keys - actual.keys) {
            changes += "removed: $path"
        }
        return changes
    }

    private fun sha256(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        DigestInputStream(file.inputStream(), digest).use {
            it.transferTo(OutputStream.nullOutputStream())
        }
        return HexFormat.of().formatHex(digest.digest())
    }
}

val testBlocksDirOverride = providers.gradleProperty("hiero.testBlocks.dir")
val testBlocksParent = file(testBlocksDirOverride.getOrElse(".test-blocks"))

// Pulls ci-test-blocks (unit tests) plus any archives given as
// -PtestBlocks=<archive>[,<archive>...]
tasks.register<FetchTestBlocks>("fetchTestBlocks") {
    group = "verification"
    description = "Downloads the pinned test block archives into $testBlocksParent"
    archives.add("ci-test-blocks")
    archives.addAll(
        providers
            .gradleProperty("testBlocks")
            .map { list ->
                list.split(",").map { it.trim() }.filter { it.isNotEmpty() }
            }
            .orElse(emptyList())
    )
    registry.set(providers.gradleProperty("hiero.testBlocks.registry"))
    pins.set(
        providers.gradlePropertiesPrefixedBy("hiero.testBlocks.image.").map { all ->
            all.mapKeys { it.key.removePrefix("hiero.testBlocks.image.") }
        }
    )
    parentDir.set(testBlocksParent)
    overridden.set(testBlocksDirOverride.isPresent)
}

/** Passes the ci-test-blocks directory to tests and makes its content a test input (cache key). */
class TestBlocksDirArgument(
    @get:InputDirectory @get:PathSensitive(PathSensitivity.RELATIVE) val dir: File
) : CommandLineArgumentProvider {
    override fun asArguments() = listOf("-Dhiero.testBlocks.dir=${dir.absolutePath}")
}

subprojects {
    tasks.withType<Test> {
        dependsOn(":fetchTestBlocks")
        jvmArgumentProviders.add(TestBlocksDirArgument(testBlocksParent.resolve("ci-test-blocks")))
    }
}

// @todo(1909) we need to remove this see issue for details
subprojects {
    tasks.withType<Test> {
        // Point docker-java/Testcontainers at the Desktop socket
        systemProperty("docker.host", "unix:///Users/user/.docker/run/docker.sock")

        // Docker 29 requires API >= 1.44
        systemProperty("api.version", "1.44")
    }
}
