// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Base"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("org.hiero.block.node.app.test.fixtures")
    requires("org.assertj.core")
    requires("org.mockito")
    requires("org.testcontainers")
    requires("io.minio")
    requires("junit")
    requires("org.hiero.block.protobuf")
}
