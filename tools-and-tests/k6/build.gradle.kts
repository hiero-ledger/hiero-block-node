// SPDX-License-Identifier: Apache-2.0
import org.hiero.gradle.spotless.LicenseHeader

plugins {
    id("com.diffplug.spotless")
}

description = "Hiero Block Node K6 Tests"

spotless {
    javascript {
        // Target all JavaScript files in the project
        target("src/**/*.js")

        prettier().config(mapOf("tabWidth" to 4))

        trimTrailingWhitespace()
        leadingTabsToSpaces()
        endWithNewline()

        // additional newline after header in 'rs' files
        licenseHeader(LicenseHeader.HEADER_STYLE_C + "\n", LicenseHeader.FIRST_LINE_REGEX_STYLE_C)
    }
}

tasks.register<Exec>("runK6Tests") {
    description = "runs the K6 tests"
    dependsOn("spotlessJavascriptCheck")
    commandLine("./run-k6-tests.sh")
}

tasks.register<Exec>("printK6Logs") {
    description = "print the K6 test logs"
    commandLine("./print-logs.sh")
}
