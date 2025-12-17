// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins {
    id("org.hiero.gradle.build") version "0.6.3"
    id("com.hedera.pbj.pbj-compiler") version "0.12.10" apply false
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
        module("state/live") { artifact = "block-node-state-live" }
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

// Patch swirlds JARs that need additional module-info configuration
@Suppress("UnstableApiUsage")
gradle.lifecycle.beforeProject {
    plugins.withId("org.hiero.gradle.module.library") {
        the<ExtraJavaModuleInfoPluginExtension>().apply {

            // Patch swirlds-common to add missing requires directives
            module("com.hedera.hashgraph:swirlds-common", "com.swirlds.common") {
                patchRealModule()
                exportAllPackages()
                requireAllDefinedDependencies()
                requires("java.desktop")
                requires("jdk.httpserver")
                requires("jdk.management")
            }

            // Replace base-utility module-info to use our protobuf-pbj instead of hapi
            module("com.hedera.hashgraph:base-utility", "org.hiero.base.utility") {
                patchRealModule() // overwrite existing module-info.class completely
                exportAllPackages()
                // Core dependencies (from original module-info, minus hapi)
                requires("com.swirlds.base")
                requires("com.swirlds.logging")
                requires("com.hedera.pbj.runtime")
                requires("io.github.classgraph")
                // Replace hapi with our protobuf-pbj
                requires("org.hiero.block.protobuf.pbj")
            }

            // Replace consensus-model module-info to use our protobuf-pbj instead of hapi
            module("com.hedera.hashgraph:consensus-model", "org.hiero.consensus.model") {
                patchRealModule() // overwrite existing module-info.class completely
                exportAllPackages()
                // Core dependencies (from original module-info, minus hapi)
                requires("com.hedera.pbj.runtime")
                requires("org.hiero.base.crypto")
                requires("org.hiero.base.utility")
                // Replace hapi with our protobuf-pbj
                requires("org.hiero.block.protobuf.pbj")
            }

            // Patch base-crypto
            module("com.hedera.hashgraph:base-crypto", "org.hiero.base.crypto") {
                patchRealModule()
                exportAllPackages()
                requireAllDefinedDependencies()
            }

            // Patch base-concurrent
            module("com.hedera.hashgraph:base-concurrent", "org.hiero.base.concurrent") {
                patchRealModule()
                exportAllPackages()
                requireAllDefinedDependencies()
            }
        }

        // Exclude hapi from all configurations - we use our own protobuf-pbj types
        // This prevents the package conflict between com.hedera.node.hapi and
        // org.hiero.block.protobuf.pbj
        configurations.all { exclude(group = "com.hedera.hashgraph", module = "hapi") }
    }
}
