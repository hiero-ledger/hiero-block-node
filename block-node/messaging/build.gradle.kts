// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Messaging Facility"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.hedera.pbj.grpc.helidon")
}

testModuleInfo {
    requires("com.hedera.pbj.runtime")
    requires("org.hiero.block.stream")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
    requires("com.swirlds.metrics.api")
    requires("com.hedera.pbj.grpc.helidon")
}
