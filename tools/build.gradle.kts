// SPDX-License-Identifier: Apache-2.0
plugins {
    id("application")
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.legacy-classpath") // due to 'com.google.cloud.storage'
    id("org.hiero.gradle.feature.shadow")
}

description = "Hiero Block Stream Tools"

application { mainClass = "org.hiero.block.tools.BlockStreamTool" }

mainModuleInfo {
    requiresStatic(
        "org.hiero.block.stream"
    ) // use streams module to access protobuf generated classes
    requires("com.hedera.pbj.runtime")
    requires("com.github.luben.zstd_jni")
    requires("com.google.api.gax")
    requires("com.google.auth.oauth2")
    requires("com.google.cloud.core")
    requires("com.google.cloud.storage")
    requires("com.google.gson")
    requires("info.picocli")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
    runtimeOnly("io.grpc.netty")
}

testModuleInfo { requiresStatic("com.github.spotbugs.annotations") }
