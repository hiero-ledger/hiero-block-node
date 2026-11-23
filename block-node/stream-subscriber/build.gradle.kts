// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Subscriber Service"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

mainModuleInfo {
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
}

testModuleInfo {
    requires("com.hedera.pbj.grpc.helidon")
    requires("com.hedera.pbj.grpc.helidon.config")
    requires("org.hiero.block.node.app.test.fixtures")
    requires("io.grpc")
    requires("io.grpc.netty")
    requires("io.grpc.stub")
    requires("io.helidon.common")
    requires("io.helidon.webserver")
    requires("io.helidon.webserver.http2")
    requires("io.netty.buffer")
    requires("io.netty.codec")
    requires("io.netty.codec.http2")
    requires("io.netty.common")
    requires("io.netty.handler")
    requires("io.netty.transport")
    requires("org.assertj.core")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
}
