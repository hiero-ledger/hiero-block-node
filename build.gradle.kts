// SPDX-License-Identifier: Apache-2.0
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.file.Files
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
 * Downloads test block flavors (tags of one OCI package, pinned in gradle.properties) into
 * .test-blocks/<flavor>/. A flavor is re-downloaded only when its pin changes; a directory with
 * unpublished changes is never replaced.
 */
abstract class FetchTestBlocks @Inject constructor(private val exec: ExecOperations) :
    DefaultTask() {
    @get:Input abstract val archives: SetProperty<String>
    @get:Input abstract val packageName: Property<String>
    @get:Input abstract val pins: MapProperty<String, String>
    @get:Internal abstract val parentDir: DirectoryProperty
    @get:Input abstract val overridden: Property<Boolean>
    @get:Input @get:Optional abstract val mountDir: Property<String>

    @TaskAction
    fun fetch() {
        val parent = parentDir.get().asFile
        for (archive in archives.get()) {
            if (!archive.matches(Regex("[a-z0-9]+(-[a-z0-9]+)*"))) {
                throw GradleException(
                    "Invalid test blocks flavor name '$archive', expected [a-z0-9]+(-[a-z0-9]+)*"
                )
            }
            val (version, pinned) = partPins(archive)
            val dir = File(parent, archive)
            if (overridden.get()) {
                if (!dir.isDirectory) {
                    throw GradleException("Test blocks directory $dir does not exist")
                }
            } else {
                val refs = pinned.map { "${packageName.get()}:$it" }
                fetchArchive(archive, version, refs, parent, dir)
            }
        }
    }

    /**
     * The version and the part pins (p1..pK) of a flavor: one pin hiero.testBlocks.image.<flavor>
     * (<flavor>-<YYYY.MM.DD>-<N>@sha256:<digest>) or pins hiero.testBlocks.image.<flavor>.p<k>
     * (<flavor>-<YYYY.MM.DD>-<N>-p<k>@sha256:<digest>) of one version. The flavor in the tag means
     * a pin cannot name another flavor.
     */
    private fun partPins(archive: String): Pair<String, List<String>> {
        val prefix = "hiero.testBlocks.image."
        val single = pins.get()[archive]
        val parts = pins.get().filterKeys { it.startsWith("$archive.p") }
        if (single == null && parts.isEmpty()) {
            throw GradleException(
                "No pin for test blocks flavor '$archive': add $prefix$archive to gradle.properties"
            )
        }
        if (single != null && parts.isNotEmpty()) {
            throw GradleException(
                "Test blocks flavor '$archive' has both $prefix$archive and part pins, keep one"
            )
        }
        val version = Regex.escape(archive) + """-\d{4}\.\d{2}\.\d{2}-\d+"""
        val digest = "@sha256:[0-9a-f]{64}"
        val keys = if (single != null) listOf(archive) else (1..parts.size).map { "$archive.p$it" }
        val versions = mutableSetOf<String>()
        for ((index, key) in keys.withIndex()) {
            val pin =
                pins.get()[key]
                    ?: throw GradleException(
                        "Part pins of '$archive' must be p1..p${parts.size} without gaps, " +
                            "missing $prefix$key"
                    )
            val part = if (single != null) "" else "-p${index + 1}"
            val match =
                Regex("($version)" + Regex.escape(part) + digest).matchEntire(pin)
                    ?: throw GradleException(
                        "Invalid pin $prefix$key=$pin, expected " +
                            "$archive-<YYYY.MM.DD>-<N>$part@sha256:<digest>"
                    )
            versions += match.groupValues[1]
        }
        if (versions.size != 1) {
            throw GradleException(
                "Part pins of '$archive' name different versions $versions, publish and pin all " +
                    "parts of one version"
            )
        }
        return versions.first() to keys.map { pins.get().getValue(it) }
    }

    private fun fetchArchive(
        archive: String,
        version: String,
        refs: List<String>,
        parent: File,
        dir: File,
    ) {
        val mounted = mountedParts(archive, version, refs.size)
        // .mounted (mount paths and version) for a tree linked to a runner mount, .image-ref
        // (part references) for a pulled one
        val stamp = File(dir, if (mounted != null) ".mounted" else ".image-ref")
        val stampText =
            mounted?.joinToString("\n", postfix = "\n$version") { it.absolutePath }
                ?: refs.joinToString("\n")
        if (!stamp.isFile || stamp.readText().trim() != stampText) {
            // a linked tree points into a read-only mount, so it cannot hold local changes
            if (dir.exists() && !File(dir, ".mounted").exists()) {
                val changes = diffAgainstManifest(dir)
                if (changes.isNotEmpty()) {
                    throw GradleException(
                        "$archive has unpublished changes; publish them as a new image version " +
                            "or delete $dir\n" +
                            changes.joinToString("\n")
                    )
                }
            }
            // fill a directory next to the target first, so a failed download never leaves a
            // half-filled flavor; the parts merge into one tree
            val tmp = File(parent, ".$archive.tmp")
            tmp.deleteRecursively()
            tmp.mkdirs()
            if (mounted != null) {
                logger.lifecycle("Linking mounted test blocks $version from ${mounted[0].parent}")
                linkParts(mounted, tmp)
            } else {
                pull(archive, version, refs, tmp)
            }
            File(tmp, stamp.name).writeText(stampText + "\n")
            // deleting a linked tree removes the links only, never the mounted files
            dir.deleteRecursively()
            if (!tmp.renameTo(dir)) {
                throw GradleException("Could not move $tmp to $dir")
            }
        }
    }

    /**
     * The runner mount's part directories <mountDir>/<flavor>/p1..pK when every one holds the
     * pinned version in its VERSION file, else null. A mount with another version logs a warning:
     * the pin is ahead of (or behind) the mount, and the pinned version is pulled instead.
     */
    private fun mountedParts(archive: String, version: String, partCount: Int): List<File>? {
        val root = mountDir.orNull?.takeIf { it.isNotBlank() }?.let { File(it, archive) }
        var parts: List<File>? = null
        if (root != null && root.isDirectory) {
            val dirs = (1..partCount).map { File(root, "p$it") }
            val found = dirs.map {
                File(it, "VERSION").takeIf { f -> f.isFile }?.readText()?.trim()
            }
            if (found.all { it == version }) {
                parts = dirs
            } else {
                val state = dirs.zip(found).joinToString { (d, v) -> "${d.name}=${v ?: "missing"}" }
                logger.warn(
                    "WARNING: mounted test blocks $root hold $state, pin is $version; pulling " +
                        "the pinned version. Ask release engineering to mount $version."
                )
            }
        }
        return parts
    }

    /**
     * Fills [tmp] with one symlink per file of the mounted parts; the files every part carries are
     * linked once.
     */
    private fun linkParts(parts: List<File>, tmp: File) {
        for (part in parts) {
            for (file in part.walk().filter { it.isFile }) {
                val link = File(tmp, file.relativeTo(part).path)
                if (!link.exists()) {
                    link.parentFile.mkdirs()
                    Files.createSymbolicLink(link.toPath(), file.absoluteFile.toPath())
                }
            }
        }
    }

    /**
     * Pulls every part into [tmp] and checks the merged tree against parts/(*).sha256 and VERSION.
     */
    private fun pull(archive: String, version: String, refs: List<String>, tmp: File) {
        for (partRef in refs) {
            logger.lifecycle("Fetching test blocks $partRef")
            exec.exec { commandLine("docker", "pull", "--quiet", partRef) }
            val out = ByteArrayOutputStream()
            exec.exec {
                commandLine("docker", "create", partRef, "/none")
                standardOutput = out
            }
            val container = out.toString().trim()
            try {
                exec.exec {
                    commandLine("docker", "cp", "$container:/$archive/.", tmp.absolutePath)
                }
            } finally {
                // best-effort cleanup: a failure here must not hide a failed docker cp
                exec.exec {
                    commandLine("docker", "rm", container)
                    isIgnoreExitValue = true
                }
                // keep Docker from holding extra copies of (possibly multi-GB) parts
                exec.exec {
                    commandLine("docker", "rmi", partRef)
                    isIgnoreExitValue = true
                }
            }
        }
        val problems = diffAgainstManifest(tmp)
        val found = File(tmp, "VERSION").takeIf { it.isFile }?.readText()?.trim()
        if (found != version) {
            problems += "VERSION is $found, expected $version"
        }
        if (problems.isNotEmpty()) {
            throw GradleException(
                refs.joinToString("\n") +
                    "\ndoes not match its parts/*.sha256 or VERSION\n" +
                    problems.joinToString("\n")
            )
        }
    }

    /**
     * Files added, removed or changed compared to the part checksum files parts/p<k>.sha256 (all
     * files if there are none). A file listed in several parts must have the same hash in each.
     */
    private fun diffAgainstManifest(dir: File): MutableList<String> {
        val partFiles =
            File(dir, "parts")
                .listFiles { f -> f.isFile && f.name.endsWith(".sha256") }
                ?.sortedBy { it.name }
                .orEmpty()
        val expected = mutableMapOf<String, String>()
        val changes = mutableListOf<String>()
        for (part in partFiles) {
            for (line in part.readLines()) {
                if (line.isNotBlank()) {
                    // sha256sum format: "<hash>  ./<path>"
                    val path = line.substring(66).removePrefix("./")
                    val hash = line.substring(0, 64)
                    val previous = expected.put(path, hash)
                    if (previous != null && previous != hash) {
                        changes += "conflict: $path differs between parts"
                    }
                }
            }
        }
        val actual =
            dir.walk()
                .filter { it.isFile && it.name != ".image-ref" }
                .map { it.relativeTo(dir).invariantSeparatorsPath }
                .filter { !it.startsWith("parts/") }
                .associateWith { sha256(File(dir, it)) }
        if (partFiles.isEmpty()) {
            changes += "missing parts/*.sha256"
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

// Pulls the ci flavor (unit tests) plus any flavors given as
// -PtestBlocks=<flavor>[,<flavor>...]
tasks.register<FetchTestBlocks>("fetchTestBlocks") {
    group = "verification"
    description = "Downloads the pinned test block flavors into $testBlocksParent"
    archives.add("ci")
    archives.addAll(
        providers
            .gradleProperty("testBlocks")
            .map { list ->
                list.split(",").map { it.trim() }.filter { it.isNotEmpty() }
            }
            .orElse(emptyList())
    )
    packageName.set(providers.gradleProperty("hiero.testBlocks.package"))
    pins.set(
        providers.gradlePropertiesPrefixedBy("hiero.testBlocks.image.").map { all ->
            all.mapKeys { it.key.removePrefix("hiero.testBlocks.image.") }
        }
    )
    parentDir.set(testBlocksParent)
    overridden.set(testBlocksDirOverride.isPresent)
    mountDir.set(providers.gradleProperty("hiero.testBlocks.mountDir"))
}

/** Passes the ci flavor directory to tests and makes its content a test input (cache key). */
class TestBlocksDirArgument(
    @get:InputDirectory @get:PathSensitive(PathSensitivity.RELATIVE) val dir: File
) : CommandLineArgumentProvider {
    override fun asArguments() = listOf("-Dhiero.testBlocks.dir=${dir.absolutePath}")
}

subprojects {
    tasks.withType<Test> {
        dependsOn(":fetchTestBlocks")
        jvmArgumentProviders.add(TestBlocksDirArgument(testBlocksParent.resolve("ci")))
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
