// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node - Cloud Expanded Plugin"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")
    requires("java.logging")
    requires("java.net.http")
    requires("jdk.httpserver")
    requires("org.junit.jupiter.api")
    requires("s3mock.testcontainers")

    runtimeOnly("org.hiero.metrics")
    runtimeOnly("org.testcontainers")
}
