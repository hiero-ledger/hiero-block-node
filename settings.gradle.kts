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
        module("protobuf")
    }
    directory("block-node") {
        group = "org.hiero.block"
        module("app") { artifact = "block-node-app" }
        module("base") { artifact = "block-node-base" }
        module("health") { artifact = "block-node-health" }
        module("messaging") { artifact = "facility-messaging" }
        module("publisher") { artifact = "block-node-publisher" }
        module("stream-subscriber") { artifact = "block-node-stream-subscriber" }
        module("verification") { artifact = "block-node-verification" }
        module("block-providers/cloud.historic") { artifact = "block-node-blocks-cloud-historic" }
        module("block-providers/files.historic") { artifact = "block-node-blocks-file-historic" }
        module("block-providers/files.recent") { artifact = "block-node-blocks-file-recent" }
        module("block-access") { artifact = "block-access-service" }
        module("s3-archive") { artifact = "s3-archive" }
    }
}
