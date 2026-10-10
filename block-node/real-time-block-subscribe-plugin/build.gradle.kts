// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Real-Time Block Subscribe Plugin"

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")
    requires("org.junit.jupiter.api")
    requires("org.mockito")
}
