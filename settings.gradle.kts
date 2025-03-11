// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.build") version "0.3.5" }

rootProject.name = "hiero-block-node"

javaModules {
    directory(".") {
        group = "org.hiero.block"
        module("tools") // no 'module-info' yet
    }
}
