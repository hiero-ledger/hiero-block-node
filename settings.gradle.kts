// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins { id("org.hiero.gradle.build") version "0.4.1" }

rootProject.name = "hiero-block-node"

javaModules {
    directory(".") {
        group = "org.hiero.block"
        module("tools") // no 'module-info' yet
        module("suites")
        module("simulator")
        module("common")
        module("protobuf") { artifact = "block-node-protobuf" }
    }
    directory("block-node") {
        group = "org.hiero.block"
        module("app") { artifact = "block-node-app" }
        module("base") { artifact = "block-node-base" }
        module("block-access") { artifact = "block-access-service" }
        module("block-providers/files.historic") { artifact = "block-node-blocks-file-historic" }
        module("block-providers/files.recent") { artifact = "block-node-blocks-file-recent" }
        module("health") { artifact = "block-node-health" }
        module("messaging") { artifact = "facility-messaging" }
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
