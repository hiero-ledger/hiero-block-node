// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.build") version "0.7.6"
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
