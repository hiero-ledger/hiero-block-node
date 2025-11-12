// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins {
    id("org.hiero.gradle.build") version "0.6.0"
    id("com.hedera.pbj.pbj-compiler") version "0.12.5" apply false
}

val hieroGroup = "org.hiero.block"

rootProject.name = "hiero-block-node"

javaModules {
    directory(".") {
        group = hieroGroup
        module("common")
        module("protobuf-sources") { artifact = "block-node-protobuf-sources" }
    }
    directory("tools-and-tests") {
        group = hieroGroup
        module("tools") // no 'module-info' yet
        module("suites")
        module("simulator")
        module("protobuf-protoc")
    }
    directory("block-node") {
        group = hieroGroup
        module("app") { artifact = "block-node-app" }
        module("app-config") { artifact = "block-node-app-config" }
        module("backfill") { artifact = "block-node-backfill" }
        module("base") { artifact = "block-node-base" }
        module("block-access") { artifact = "block-access-service" }
        module("block-providers/files.historic") { artifact = "block-node-blocks-file-historic" }
        module("block-providers/files.recent") { artifact = "block-node-blocks-file-recent" }
        module("health") { artifact = "block-node-health" }
        module("messaging") { artifact = "facility-messaging" }
        module("protobuf-pbj") { artifact = "block-node-protobuf-pbj" }
        module("s3-archive") { artifact = "block-node-s3-archive" }
        module("server-status") { artifact = "block-node-server-status" }
        module("spi") { artifact = "block-node-spi-plugins" }
        module("stream-publisher") { artifact = "block-node-stream-publisher" }
        module("stream-subscriber") { artifact = "block-node-stream-subscriber" }
        module("verification") { artifact = "block-node-verification" }
    }
}

// @jjohannes: remove once 'swirldsVersion' is updated to '0.63.x' in
// hiero-dependency-versions/build.gradle.kts
@Suppress("UnstableApiUsage")
gradle.lifecycle.beforeProject {
    plugins.withId("org.hiero.gradle.module.library") {
        the<ExtraJavaModuleInfoPluginExtension>().apply {
            // rewrite requires: io.prometheus.simpleclient -> simpleclient
            // https://github.com/hiero-ledger/hiero-gradle-conventions/pull/178
            // https://github.com/hiero-ledger/hiero-consensus-node/pull/19059
            module("com.swirlds:swirlds-common", "com.swirlds.common") {
                patchRealModule()
                exportAllPackages()
                requireAllDefinedDependencies()
                requires("java.desktop")
                requires("jdk.httpserver")
                requires("jdk.management")
            }
        }
    }
}
