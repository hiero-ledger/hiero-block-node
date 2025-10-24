// SPDX-License-Identifier: Apache-2.0
dependencies {
    published(project(":block-node-protobuf-sources"))

    implementation(project(":block-node-app"))
    implementation(project(":simulator"))
    implementation(project(":suites"))
    implementation(project(":tools"))
}
