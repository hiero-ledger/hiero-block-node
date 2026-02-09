// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins {
    id("org.hiero.gradle.build") version "0.7.0"
    id("com.hedera.pbj.pbj-compiler") version "0.14.0" apply false
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
