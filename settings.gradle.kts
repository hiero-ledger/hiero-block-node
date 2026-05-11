// SPDX-License-Identifier: Apache-2.0
import org.gradlex.javamodule.moduleinfo.ExtraJavaModuleInfoPluginExtension

plugins {
    id("org.hiero.gradle.build") version "0.7.7"
    id("com.hedera.pbj.pbj-compiler") version "0.15.2" apply false
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
        }
    }
}
