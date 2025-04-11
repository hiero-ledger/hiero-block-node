// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Commons module with logic that could be abstracted and reused."

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

testModuleInfo {
    requiresStatic("com.github.spotbugs.annotations")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.assertj.core")
    requires("com.google.common.jimfs")
}
