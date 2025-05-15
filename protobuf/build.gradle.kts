// SPDX-License-Identifier: Apache-2.0
import org.hiero.gradle.tasks.GitClone

plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.protobuf")
    // When upgrading pbjVersion, also need to update pbjVersion on
    // hiero-dependency-versions/build.gradle.kts
    id("com.hedera.pbj.pbj-compiler") version "0.11.3"
}

description = "Hiero Block Node Protobuf API"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-exports,-deprecation,-removal,-dep-ann")
}

val exportBlockNodeProto: TaskProvider<Copy> =
    tasks.register<Copy>("exportBlockNodeProto") {
        description =
            "Copies the protobuf files from block api to a combined CN & BN block-streams related protobuf directory"
        group = "protobuf"

        from(layout.projectDirectory.dir("src/main/proto/org/hiero/block/api"))
        into(layout.buildDirectory.dir("block-node-protobuf"))
        dependsOn(exportConsensusNodeBlockProto)
        dependsOn(exportConsensusNodePlatformProto)
        dependsOn(exportConsensusNodeServicesProto)
        dependsOn(exportConsensusNodeStreamsProto)
    }

val exportConsensusNodeBlockProto =
    tasks.register<Copy>("exportConsensusNodeBlockProto") {
        description =
            "Copies the protobuf files from block api to a combined CN & BN block-streams related protobuf directory"
        group = "protobuf"

        from(
            layout.buildDirectory.dir(
                "hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/block"
            )
        )
        into(layout.buildDirectory.dir("block-node-protobuf/block"))
    }

val exportConsensusNodePlatformProto =
    tasks.register<Copy>("exportConsensusNodePlatformProto") {
        description =
            "Copies the CN platorm protobuf files to a combined CN & BN block-streams related protobuf directory"
        group = "protobuf"

        from(
            layout.buildDirectory.dir(
                "hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/platform"
            )
        )
        into(layout.buildDirectory.dir("block-node-protobuf/platform"))
    }

val exportConsensusNodeServicesProto =
    tasks.register<Copy>("exportConsensusNodeServicesProto") {
        description =
            "Copies the CN services protobuf files to a combined CN & BN block-streams related protobuf directory"
        group = "protobuf"

        from(
            layout.buildDirectory.dir(
                "hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/services"
            )
        )
        into(layout.buildDirectory.dir("block-node-protobuf/services"))
    }

val exportConsensusNodeStreamsProto =
    tasks.register<Copy>("exportConsensusNodeStreamsProto") {
        description =
            "Copies the CN streams protobuf files to a combined CN & BN block-streams related protobuf directory"
        group = "protobuf"

        from(
            layout.buildDirectory.dir(
                "hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/streams"
            )
        )
        into(layout.buildDirectory.dir("block-node-protobuf/streams"))
    }

// Add downloaded HAPI repo protobuf files into build directory and add to sources to build them
val cloneHederaProtobufs =
    tasks.register<GitClone>("cloneHederaProtobufs") {
        url = "https://github.com/hiero-ledger/hiero-consensus-node.git"
        localCloneDirectory = layout.buildDirectory.dir("hiero-consensus-node")

        // uncomment below to use a specific tag
        // tag = "v0.53.0" or a specific commit like "0047255"
        tag = "efb0134e921b32ed6302da9c93874d65492e876f"

        // uncomment below to use a specific branch
        // branch = "main"

        // remove the block_service.proto file pulled from hedera-protobufs in favour of local
        // version
        doLast {
            localCloneDirectory
                .file("hapi/hedera-protobuf-java-api/src/main/proto/block/block_service.proto")
                .get()
                .asFile
                .delete()
        }
        doLast {
            localCloneDirectory
                .file(
                    "hapi/hedera-protobuf-java-api/src/main/proto/mirror/mirror_network_service.proto"
                )
                .get()
                .asFile
                .delete()
        }
        doLast {
            localCloneDirectory
                .file("hapi/hedera-protobuf-java-api/src/main/proto/sdk/transaction_list.proto")
                .get()
                .asFile
                .delete()
        }
    }

sourceSets {
    main {
        pbj {
            srcDir(
                cloneHederaProtobufs.map {
                    it.localCloneDirectory.dir("hapi/hedera-protobuf-java-api/src/main/proto")
                }
            )
        }
        proto {
            srcDir(
                cloneHederaProtobufs.map {
                    it.localCloneDirectory.dir("hapi/hedera-protobuf-java-api/src/main/proto")
                }
            )
        }
    }
}

tasks.test {
    // we can exclude the standard protobuf generated tests as we don't need to test them again here
    // this speeds up the block node project test run no end :-)
    exclude("**com/hedera/**")
}

testModuleInfo {
    // we depend on the protoc compiled hapi during test as we test our pbj generated code
    // against it to make sure it is compatible
    requires("com.google.protobuf.util")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
}
