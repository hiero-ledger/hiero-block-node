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
        module("app") { artifact = "block-node-app" }
        module("base") { artifact = "block-node-base" }
        module("plugins") { artifact = "block-node-plugins" }
        module("health") { artifact = "block-node-health" }
        module("messaging") { artifact = "facility-messaging" }
        module("publisher") { artifact = "block-node-publisher" }
        module("subscriber") { artifact = "block-node-subscriber" }
        module("verification") { artifact = "block-node-verification" }
        // module("persistence") { artifact = "service-persistence" }
        // module("persistence-api") { artifact = "service-persistence-api" }
        // module("configuration") { artifact = "facility-configuration" }
        // module("registrar") { artifact = "facility-registrar" }
        // module("facilities-api") { artifact = "facilities-api" }
    }
}
