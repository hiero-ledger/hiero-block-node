// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.build") version "0.3.8" }

rootProject.name = "hiero-block-node"

javaModules {
    directory(".") {
        group = "org.hiero.block"
        module("tools") // no 'module-info' yet
        module("suites")
        module("simulator")
        module("common")
        module("stream")
    }
    directory("block-node") {
        group = "org.hiero.block"
        module("server") { artifact = "block-node-server" }
        module("base") { artifact = "block-node-base" }
        module("plugins") { artifact = "block-node-plugins" }
        // module("persistence") { artifact = "service-persistence" }
        // module("persistence-api") { artifact = "service-persistence-api" }
        // module("verification") { artifact = "service-verification" }
        // module("verification-api") { artifact = "service-verification-api" }
        module("messaging") { artifact = "facility-messaging" }
        // module("configuration") { artifact = "facility-configuration" }
        // module("registrar") { artifact = "facility-registrar" }
        // module("facilities-api") { artifact = "facilities-api" }
    }
}
