// SPDX-License-Identifier: Apache-2.0
dependencies {
    published(project(":protobuf-pbj"))
    published(project(":app"))
    published(project(":app-config"))
    published(project(":backfill"))
    published(project(":base"))
    published(project(":block-access-service"))
    published(project(":blocks-file-historic"))
    published(project(":blocks-file-recent"))
    published(project(":health"))
    published(project(":facility-messaging"))
    published(project(":s3-archive"))
    published(project(":server-status"))
    published(project(":spi-plugins"))
    published(project(":stream-publisher"))
    published(project(":stream-subscriber"))
    published(project(":verification"))

    implementation(project(":app"))
    implementation(project(":simulator"))
    implementation(project(":suites"))
    implementation(project(":tools"))
}
