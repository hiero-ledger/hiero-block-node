// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node State Management Hashgraph Plugin (beta)"

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.assertj.core")
    requires("org.hiero.block.node.app.test.fixtures")
    requires("com.swirlds.config.api")
    requires("com.swirlds.merkledb")
    requires("com.swirlds.virtualmap")
    requires("org.hiero.consensus.utility")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("com.swirlds.config.extensions")
}
