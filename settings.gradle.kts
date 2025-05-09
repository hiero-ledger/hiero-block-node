// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.build") version "0.4.0" }

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
        module("publisher") { artifact = "block-node-publisher" }
        module("s3-archive") { artifact = "block-node-s3-archive" }
        module("server-status") { artifact = "block-node-server-status" }
        module("spi") { artifact = "block-node-spi-plugins" }
        module("stream-subscriber") { artifact = "block-node-stream-subscriber" }
        module("verification") { artifact = "block-node-verification" }
    }
}
