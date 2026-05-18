// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("com.hedera.pbj.pbj-compiler")
}

description = "Hiero Block Node RSA Roster Bootstrap Plugin"

tasks.javadoc {
    options {
        this as StandardJavadocDocletOptions
        // There are violations in the generated pbj code
        addStringOption("Xdoclint:-reference,-html", "-quiet")
    }
}

pbj { generateTestClasses = false }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.hedera.pbj.grpc.client.helidon")
    runtimeOnly("com.hedera.pbj.grpc.helidon")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.hiero.block.node.app.test.fixtures")
    requires("jdk.httpserver")
}
