// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins {
    id("org.hiero.gradle.build") version "0.7.10"
    id("com.hedera.pbj.pbj-compiler") version "0.15.8" apply false
}

val hieroGroup = "org.hiero.block-node"

rootProject.name = "hiero-block-node"

javaModules {
    directory(".") { group = hieroGroup }
    directory("tools-and-tests") {
        group = hieroGroup
        module("tools") // no 'module-info' yet
    }
    directory("block-node") { group = hieroGroup }
}

include("k6-tests")

project(":k6-tests").projectDir = file("tools-and-tests/k6")

@Suppress("UnstableApiUsage")
gradle.lifecycle.beforeProject {
    plugins.withId("org.hiero.gradle.module.library") {
        the<ExtraJavaModuleInfoPluginExtension>().apply {
            // s3mock-testcontainers has no module-info; add synthetic module descriptor
            module("com.adobe.testing:s3mock-testcontainers", "s3mock.testcontainers") {
                exportAllPackages()
                requires("org.testcontainers")
                requires("org.apache.commons.compress")
            }

            // Non-modular jars pulled in transitively by swirlds-state-impl / virtualmap.
            module("com.goterl:lazysodium-java", "com.goterl.lazysodium") {
                exportAllPackages()
                requires("com.sun.jna")
                requires("com.goterl.resourceloader")
            }
            module("com.goterl:resource-loader", "com.goterl.resourceloader") {
                exportAllPackages()
                requires("com.sun.jna")
            }
            module("net.java.dev.jna:jna", "com.sun.jna") { exportAllPackages() }
            module("io.prometheus:simpleclient", "simpleclient") {
                exportAllPackages()
                requires("simpleclient.tracer.common")
            }
            module("io.prometheus:simpleclient_common", "simpleclient.common") {
                exportAllPackages()
                requires("simpleclient")
            }
            module("io.prometheus:simpleclient_httpserver", "simpleclient.httpserver") {
                exportAllPackages()
                requires("simpleclient")
                requires("simpleclient.common")
            }
            module("io.prometheus:simpleclient_tracer_common", "simpleclient.tracer.common") {
                exportAllPackages()
            }
            module(
                "org.hyperledger.besu:besu-native-common",
                "org.hyperledger.besu.nativelib.common",
            ) {
                exportAllPackages()
            }
            module("org.hyperledger.besu:secp256k1", "org.hyperledger.besu.nativelib.secp256k1") {
                exportAllPackages()
                requires("org.hyperledger.besu.nativelib.common")
            }

            // consensus-model and consensus-utility published module-infos require
            // com.hedera.node.hapi; we substitute our protobuf-pbj to avoid the package
            // collision.
            module("com.hedera.hashgraph:consensus-model", "org.hiero.consensus.model") {
                patchRealModule()
                exportAllPackages()
                requires("com.hedera.pbj.runtime")
                requires("com.swirlds.base")
                requires("org.hiero.base.concurrent")
                requires("org.hiero.base.crypto")
                requires("org.hiero.base.utility")
                requires("org.hiero.block.protobuf.pbj")
                requiresStatic("com.github.spotbugs.annotations")
            }
            module("com.hedera.hashgraph:consensus-utility", "org.hiero.consensus.utility") {
                patchRealModule()
                exportAllPackages()
                requires("com.hedera.pbj.runtime")
                requires("com.swirlds.base")
                requires("com.swirlds.config.api")
                requires("com.swirlds.logging")
                requires("com.swirlds.metrics.api")
                requires("org.apache.logging.log4j")
                requires("org.hiero.base.concurrent")
                requires("org.hiero.base.crypto")
                requires("org.hiero.base.utility")
                requires("org.hiero.block.protobuf.pbj")
                requires("org.hiero.consensus.concurrent")
                requires("org.hiero.consensus.metrics")
                requires("org.hiero.consensus.model")
                requiresStatic("com.github.spotbugs.annotations")
            }
        }

        // Use our protobuf-pbj rather than the upstream hapi jar so the package
        // namespaces don't collide.
        configurations.all { exclude(group = "com.hedera.hashgraph", module = "hapi") }
    }
}
