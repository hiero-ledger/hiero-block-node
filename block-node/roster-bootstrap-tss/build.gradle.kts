// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Roster Bootstrap Tss Service"

dependencies { implementation(project(":base")) }

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo { runtimeOnly("com.swirlds.config.impl") }

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.hiero.block.node.app.test.fixtures")
}
