// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node RSA Roster Bootstrap Plugin"

mainModuleInfo { runtimeOnly("com.swirlds.config.impl") }

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.hiero.block.node.app.test.fixtures")
    requires("jdk.httpserver")
}
