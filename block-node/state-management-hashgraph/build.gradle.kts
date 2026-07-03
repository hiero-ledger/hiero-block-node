// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node State Management Hashgraph Plugin (beta)"

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")
    requires("io.helidon.webserver")
    requires("org.assertj.core")
    requires("org.junit.jupiter.api")

    runtimeOnly("com.swirlds.config.extensions")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.hiero.metrics")
}
