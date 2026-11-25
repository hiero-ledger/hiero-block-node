// SPDX-License-Identifier: Apache-2.0
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.security.DigestInputStream
import java.security.MessageDigest
import java.util.HexFormat
import java.util.Locale
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

    companion object {
        fun sha256(file: File): String {
            val digest = MessageDigest.getInstance("SHA-256")
            DigestInputStream(file.inputStream(), digest).use {
                it.transferTo(OutputStream.nullOutputStream())
            }
            return HexFormat.of().formatHex(digest.digest())
        }
    }
}

/**
 * Publishes a new version of a test blocks flavor from the directory <work-dir>/<flavor>/ prepared
 * in steps 1 and 2 of "Changing test blocks" in docs/test-blocks.md, and runs steps 4 and 5: splits
 * it into parts, builds one reproducible image per part and, with push, pushes the parts and pins
 * them in gradle.properties. Without push it only prints the digests a push would give.
 */
abstract class PublishTestBlocks @Inject constructor(private val exec: ExecOperations) :
    DefaultTask() {
    @get:Internal abstract val source: Property<String>
    @get:Internal abstract val push: Property<Boolean>
    @get:Internal abstract val packageName: Property<String>
    @get:Internal abstract val buildkitImage: Property<String>
    @get:Internal abstract val projectDir: DirectoryProperty
    @get:Internal abstract val workDir: DirectoryProperty
    @get:Internal abstract val mountDir: Property<String>

    private val builder = "hiero-test-blocks"
    private val partLimit = 9_500_000_000L

    @TaskAction
    fun publish() {
        val path =
            source.orNull ?: throw GradleException("Set -PtestBlocks.source=<work-dir>/<flavor>")
        val src = projectDir.get().asFile.resolve(path).canonicalFile
        val flavor = src.name
        if (!flavor.matches(Regex("[a-z0-9]+(-[a-z0-9]+)*"))) {
            throw GradleException(
                "$src: the directory name is the flavor and must match [a-z0-9]+(-[a-z0-9]+)*"
            )
        }
        val version = File(src, "VERSION").takeIf { it.isFile }?.readText()?.trim()
        if (
            version == null ||
                !version.matches(Regex(Regex.escape(flavor) + """-\d{4}\.\d{2}\.\d{2}-\d+"""))
        ) {
            throw GradleException("$src/VERSION must hold $flavor-<YYYY.MM.DD>-<N>, found $version")
        }
        // the files fetchTestBlocks adds are not part of the flavor
        val files =
            src.walk()
                .onEnter { it == src || it.relativeTo(src).path != "parts" }
                .filter { it.isFile && it.name != ".image-ref" && it.name != ".mounted" }
                .toList()
        val (blocks, others) = files.partition { it.name.contains(".blk") }
        val parts = split(src, blocks)
        val tags = parts.indices.map { if (parts.size == 1) version else "$version-p${it + 1}" }
        val pkg = packageName.get()
        // a part whose tag exists with the same digest is not pushed again, so a push that
        // failed after some parts can be re-run; only other content under the tag fails
        val existing = tags.associateWith { remoteDigest("$pkg:$it") }
        ensureBuilder()
        val pins = mutableListOf<String>()
        val sizes = mutableListOf<Long>()
        for ((index, part) in parts.withIndex()) {
            val tag = tags[index]
            val remote = existing[tag]
            val pushPart = push.get() && remote == null
            val digest = buildPart(src, flavor, index + 1, part + others, "$pkg:$tag", pushPart)
            if (remote != null && remote != digest) {
                val message =
                    "$pkg:$tag exists with $remote but this tree gives $digest; tags are never " +
                        "re-pushed, bump <N> in VERSION"
                if (push.get()) {
                    throw GradleException(message)
                }
                logger.warn("WARNING: $message")
            } else if (remote != null) {
                logger.lifecycle("$pkg:$tag already exists with the same digest, not pushed")
            }
            pins += "$tag@$digest"
            sizes += (part + others).sumOf { it.length() }
        }
        val keys =
            if (parts.size == 1) listOf(flavor) else parts.indices.map { "$flavor.p${it + 1}" }
        val pinLines = keys.zip(pins) { key, pin -> "hiero.testBlocks.image.$key=$pin" }
        if (push.get()) {
            pinInProperties(flavor, pinLines)
            logger.lifecycle("Pinned in gradle.properties:\n" + pinLines.joinToString("\n"))
            logger.lifecycle(mountRequest(flavor, version, pkg, pins, sizes))
        } else {
            logger.lifecycle(
                "Dry run, nothing pushed. A push gives:\n" + pinLines.joinToString("\n")
            )
        }
    }

    /** Block files by set folder and block number, cut into parts of at most [partLimit] bytes. */
    private fun split(src: File, blocks: List<File>): List<List<File>> {
        val sorted =
            blocks.sortedWith(
                compareBy(
                    { it.parentFile.relativeTo(src).invariantSeparatorsPath },
                    { it.name.substringBefore('.').toLongOrNull() ?: Long.MAX_VALUE },
                    { it.name },
                )
            )
        val parts = mutableListOf(mutableListOf<File>())
        var size = 0L
        for (block in sorted) {
            if (size > 0 && size + block.length() > partLimit) {
                parts += mutableListOf<File>()
                size = 0
            }
            parts.last() += block
            size += block.length()
        }
        return parts
    }

    /**
     * The digest of [ref] in the registry, or null when the registry says it does not exist. Any
     * other failure (network, rate limit) stops the task, so it never pushes over a tag it could
     * not see.
     */
    private fun remoteDigest(ref: String): String? {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val inspect = exec.exec {
            commandLine(
                "docker",
                "buildx",
                "imagetools",
                "inspect",
                ref,
                "--format",
                "{{json .Manifest.Digest}}",
            )
            standardOutput = out
            errorOutput = err
            isIgnoreExitValue = true
        }
        if (inspect.exitValue != 0 && !err.toString().contains("$ref: not found")) {
            throw GradleException("Could not check whether $ref exists:\n$err")
        }
        return if (inspect.exitValue == 0) out.toString().trim().removeSurrounding("\"") else null
    }

    /**
     * Creates the docker-container builder with the pinned BuildKit, so the digests come from
     * BuildKit and not from the local Docker image store or the daemon's BuildKit version.
     */
    private fun ensureBuilder() {
        val out = ByteArrayOutputStream()
        val inspect = exec.exec {
            commandLine("docker", "buildx", "inspect", builder)
            standardOutput = out
            errorOutput = OutputStream.nullOutputStream()
            isIgnoreExitValue = true
        }
        if (inspect.exitValue != 0) {
            exec.exec {
                commandLine(
                    "docker",
                    "buildx",
                    "create",
                    "--name",
                    builder,
                    "--driver",
                    "docker-container",
                    "--driver-opt",
                    "image=${buildkitImage.get()}",
                )
                standardOutput = OutputStream.nullOutputStream()
            }
        } else if (!out.toString().contains(buildkitImage.get())) {
            throw GradleException(
                "Builder $builder does not run ${buildkitImage.get()}; remove it with " +
                    "'docker buildx rm $builder' and run again"
            )
        }
    }

    /**
     * Copies one part into the work directory with normalized modes and its parts/p<k>.sha256,
     * builds it (and pushes it with [pushPart]), deletes the copy and returns the image digest.
     */
    private fun buildPart(
        src: File,
        flavor: String,
        k: Int,
        files: List<File>,
        ref: String,
        pushPart: Boolean,
    ): String {
        val partDir = workDir.get().asFile.resolve("p$k")
        partDir.deleteRecursively()
        val root = File(partDir, flavor)
        val paths = files.map { it.relativeTo(src).invariantSeparatorsPath }.sorted()
        for (path in paths) {
            val target = File(root, path)
            target.parentFile.mkdirs()
            Files.copy(File(src, path).toPath(), target.toPath())
        }
        // sha256sum format, sorted by path, relative to the flavor root
        File(root, "parts").mkdirs()
        File(root, "parts/p$k.sha256")
            .writeText(
                paths.joinToString("") { "${FetchTestBlocks.sha256(File(root, it))}  ./$it\n" }
            )
        for (f in root.walk()) {
            Files.setPosixFilePermissions(
                f.toPath(),
                PosixFilePermissions.fromString(if (f.isDirectory) "rwxr-xr-x" else "rw-r--r--"),
            )
        }
        File(partDir, "Dockerfile")
            .writeText(
                """
                FROM scratch
                LABEL org.opencontainers.image.source="https://github.com/hiero-ledger/hiero-block-node"
                LABEL org.opencontainers.image.description="Hiero Block Node test blocks: $flavor"
                LABEL org.opencontainers.image.licenses="Apache-2.0"
                COPY $flavor /$flavor
                """
                    .trimIndent() + "\n"
            )
        val metadata = File(partDir.parentFile, "p$k.metadata.json")
        logger.lifecycle("Building test blocks $ref (push=$pushPart)")
        try {
            exec.exec {
                environment("SOURCE_DATE_EPOCH", "0")
                commandLine(
                    "docker",
                    "buildx",
                    "build",
                    "--builder",
                    builder,
                    "--platform",
                    "linux/amd64",
                    "--no-cache",
                    "--provenance=false",
                    "--sbom=false",
                    "--quiet",
                    "--metadata-file",
                    metadata.absolutePath,
                    "--output",
                    "type=image,name=$ref,push=$pushPart,rewrite-timestamp=true",
                    partDir.absolutePath,
                )
                standardOutput = OutputStream.nullOutputStream()
            }
        } finally {
            partDir.deleteRecursively()
            // drop the part from the builder's cache before the next one
            exec.exec {
                commandLine("docker", "buildx", "prune", "--builder", builder, "--all", "--force")
                standardOutput = OutputStream.nullOutputStream()
                isIgnoreExitValue = true
            }
        }
        return Regex(""""containerimage\.digest"\s*:\s*"(sha256:[0-9a-f]{64})"""")
            .find(metadata.readText())
            ?.groupValues
            ?.get(1) ?: throw GradleException("No image digest in $metadata")
    }

    /** Replaces the flavor's pins in gradle.properties, in place of the first old pin. */
    private fun pinInProperties(flavor: String, pinLines: List<String>) {
        val props = projectDir.get().asFile.resolve("gradle.properties")
        val key = Regex("^hiero\\.testBlocks\\.image\\.${Regex.escape(flavor)}(\\.p\\d+)?=")
        val lines = props.readLines()
        val at =
            lines.indexOfFirst { key.containsMatchIn(it) }.let { if (it < 0) lines.size else it }
        val kept = lines.filterNot { key.containsMatchIn(it) }
        val updated = kept.subList(0, at) + pinLines + kept.subList(at, kept.size)
        props.writeText(updated.joinToString("\n", postfix = "\n"))
    }

    /** The mount request for release engineering (step 7 of "Changing test blocks"). */
    private fun mountRequest(
        flavor: String,
        version: String,
        pkg: String,
        pins: List<String>,
        sizes: List<Long>,
    ): String {
        val lines =
            pins.indices.joinToString("\n") {
                val (tag, digest) = pins[it].split("@")
                "  $tag  $digest  %.1f GB  -> ${mountDir.get()}/$flavor/p${it + 1}/"
                    .format(Locale.ROOT, sizes[it] / 1e9)
            }
        return """
            |If $flavor is mounted on the CI runners, ask release engineering after the PR merges:
            |Title: Mount test-blocks $version (${pins.size} parts) on BN runners
            |Package: $pkg
            |Parts (tag / digest / size, one layer each):
            |$lines
            |Mount: read-only, subPath $flavor; replaces all parts of the previous version (remove parts that no longer exist)
            |Pinned by: <PR link> (merged)
            |Previous version to remove: <previous $flavor version>
            """
            .trimMargin()
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

// ./gradlew publishTestBlocks -PtestBlocks.source=<work-dir>/<flavor> [-PtestBlocks.push=true]
tasks.register<PublishTestBlocks>("publishTestBlocks") {
    group = "publishing"
    description = "Builds (and with -PtestBlocks.push=true pushes and pins) a test blocks flavor"
    source.set(providers.gradleProperty("testBlocks.source"))
    push.set(providers.gradleProperty("testBlocks.push").map { it.toBoolean() }.orElse(false))
    packageName.set(providers.gradleProperty("hiero.testBlocks.package"))
    mountDir.set(providers.gradleProperty("hiero.testBlocks.mountDir"))
    // the builder's BuildKit decides layer compression, so it is pinned for reproducible digests
    buildkitImage.set(
        "moby/buildkit:v0.34.0@sha256:b059f8d7226d0b326bc871af489a5dddf65e36af4d20d14251b072974d9ee87c"
    )
    projectDir.set(layout.projectDirectory)
    workDir.set(layout.buildDirectory.dir("test-blocks-publish"))
    doNotTrackState("Builds and pushes images")
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
