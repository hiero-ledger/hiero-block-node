// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins {
    id("org.hiero.gradle.build") version "0.6.3"
    id("com.hedera.pbj.pbj-compiler") version "0.12.10" apply false
}

val hieroGroup = "org.hiero.block-node"

rootProject.name = "hiero-block-node"

javaModules {
    directory(".") {
        group = hieroGroup
        module("common")
        module("protobuf-sources") { artifact = "protobuf-sources" }
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
        module("app") { artifact = "app" }
        module("app-config") { artifact = "app-config" }
        module("backfill") { artifact = "backfill" }
        module("base") { artifact = "base" }
        module("block-access") { artifact = "block-access-service" }
        module("block-providers/files.historic") { artifact = "blocks-file-historic" }
        module("block-providers/files.recent") { artifact = "blocks-file-recent" }
        module("health") { artifact = "health" }
        module("messaging") { artifact = "facility-messaging" }
        module("protobuf-pbj") { artifact = "protobuf-pbj" }
        module("s3-archive") { artifact = "s3-archive" }
        module("server-status") { artifact = "server-status" }
        module("spi") { artifact = "spi-plugins" }
        module("stream-publisher") { artifact = "stream-publisher" }
        module("stream-subscriber") { artifact = "stream-subscriber" }
        module("verification") { artifact = "verification" }
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
