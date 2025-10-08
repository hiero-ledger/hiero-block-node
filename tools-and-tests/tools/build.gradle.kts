// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.application")
    id("org.hiero.gradle.feature.legacy-classpath") // due to 'com.google.cloud.storage'
    id("org.hiero.gradle.feature.shadow")
}

description = "Hiero Block Stream Tools"

application { mainClass = "org.hiero.block.tools.BlockStreamTool" }

mainModuleInfo {
    requires("org.hiero.block.protobuf.pbj")
    requires("com.hedera.pbj.runtime")
    requires("com.github.luben.zstd_jni")
    requires("com.google.api.gax")
    requires("com.google.auth.oauth2")
    requires("com.google.cloud.core")
    requires("com.google.cloud.storage")
    requires("com.google.gson")
    requires("info.picocli")
    requires("org.apache.commons.compress")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.grpc.netty")
}
