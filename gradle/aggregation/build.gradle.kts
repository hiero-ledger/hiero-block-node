// SPDX-License-Identifier: Apache-2.0
dependencies {
    published(project(":block-node-protobuf-sources"))
    published(project(":block-node-backfill"))
    published(project(":block-node-base"))
    published(project(":block-access-service"))
    published(project(":block-node-blocks-file-historic"))
    published(project(":block-node-blocks-file-recent"))
    published(project(":block-node-health"))
    published(project(":facility-messaging"))
    published(project(":block-node-s3-archive"))
    published(project(":block-node-server-status"))
    published(project(":block-node-spi-plugins"))
    published(project(":block-node-stream-publisher"))
    published(project(":block-node-stream-subscriber"))
    published(project(":block-node-verification"))

    implementation(project(":block-node-app"))
    implementation(project(":simulator"))
    implementation(project(":suites"))
    implementation(project(":tools"))
}
