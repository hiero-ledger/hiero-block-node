// SPDX-License-Identifier: Apache-2.0
import net.swiftzer.semver.SemVer

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

// @todo(1909) we need to remove this see issue for details
subprojects {
    tasks.withType<Test> {
        // Point docker-java/Testcontainers at the Desktop socket
        systemProperty("docker.host", "unix:///Users/user/.docker/run/docker.sock")

        // Docker 29 requires API >= 1.44
        systemProperty("api.version", "1.44")
    }
}
