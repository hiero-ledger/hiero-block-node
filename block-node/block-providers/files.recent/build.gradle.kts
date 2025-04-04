// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node - Blocks File Recent Provider"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    requires("com.google.common.jimfs")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.mockito")
    requires("org.mockito.junit.jupiter")
    requires("org.assertj.core")
    requires("org.hiero.block.node.app.test.fixtures")
}
