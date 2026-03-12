// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.build") version "0.7.4"
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
