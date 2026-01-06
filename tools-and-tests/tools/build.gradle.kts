// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.application")
    id("org.hiero.gradle.feature.legacy-classpath") // due to 'com.google.cloud.storage'
    id("org.hiero.gradle.feature.shadow")
}

description = "Hiero Block Stream Tools"

application { mainClass = "org.hiero.block.tools.BlockStreamTool" }

dependencies {
    // Explicit failureaccess dependency - ensures Guava internal classes are available
    // for gRPC/OAuth background threads (safety net for classloader edge cases)
    implementation("com.google.guava:failureaccess:1.0.3")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.3")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.3")
}

tasks.test { useJUnitPlatform() }

mainModuleInfo {
    requires("org.hiero.block.protobuf.pbj")
    requires("org.hiero.block.node.base")
    requires("com.hedera.pbj.runtime")
    requires("com.github.luben.zstd_jni")
    requires("com.google.api.gax")
    requires("com.google.auth.oauth2")
    requires("com.google.cloud.core")
    requires("com.google.cloud.storage")
    requires("com.google.gson")
    requires("info.picocli")
    requires("org.apache.commons.compress")
    requires("com.google.common.jimfs")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.grpc.netty")
}
