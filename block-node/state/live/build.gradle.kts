// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Live State Service"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

tasks.withType<Test>().configureEach {
    jvmArgs("--add-reads", "org.hiero.base.utility=jdk.unsupported")
}

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("com.swirlds.virtualmap")
    requires("com.swirlds.state.api")
    requires("com.swirlds.state.impl")
    requires("com.swirlds.merkledb")
    requires("com.swirlds.config.api")
    requires("com.swirlds.config.impl")
    requires("com.swirlds.common")
    requires("com.swirlds.metrics.api")
    requires("org.hiero.block.protobuf.pbj")
    requires("com.hedera.pbj.runtime")
}
