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

val cloneCNRepo =
    tasks.register<Exec>("cloneCNRepo") {
        workingDir = layout.buildDirectory.asFile.get()
        commandLine("git", "clone", "https://github.com/hiero-ledger/hiero-consensus-node.git")
        workingDir = workingDir.resolve("hiero-consensus-node")
        commandLine("cd", "hiero-consensus-node")
        commandLine("git", "checkout", "efb0134e921b32ed6302da9c93874d65492e876f")
    }

// Add downloaded HAPI repo protobuf files into build directory and add to sources to build them
val cloneHederaProtobufs =
    tasks.register<GitClone>("cloneHederaProtobufs") {
        url = "https://github.com/hiero-ledger/hiero-consensus-node.git"
        localCloneDirectory = layout.buildDirectory.dir("hedera-protobufs")

        // tag version or specific commit hash, current versions is v0.62.2
        // tag = "efb0134e921b32ed6302da9c93874d65492e876f"

        // uncomment below to use a specific branch
        branch = "main"

        //        delete(
        //            fileTree(localCloneDirectory).matching {
        //                exclude(
        //                    "hapi/hedera-protobuf-java-api/src/main/proto/block/**.proto",
        //                    "hapi/hedera-protobuf-java-api/src/main/proto/platform/**.proto",
        //                    "hapi/hedera-protobuf-java-api/src/main/proto/services/**.proto",
        //                    "hapi/hedera-protobuf-java-api/src/main/proto/streams/**.proto",
        //                )
        //            }
        //        )
    }

// val exportProto = {
//    tasks.register<Copy>("exportBlockNodeApiProto") {
//        description =
//            "Copies the protobuf files from block api to a combined CN & BN block-streams related
// protobuf directory"
//        group = "protobuf"
//
//        from(layout.projectDirectory.dir("src/main/proto/org/hiero/block/api"))
//        into(layout.buildDirectory.dir("block-node-protobuf"))
//        mustRunAfter(tasks.generateProto)
//    }
//
//    tasks.register<Copy>("exportConsensusNodeBlockProto") {
//        description =
//            "Copies the protobuf files from block api to a combined CN & BN block-streams related
// protobuf directory"
//        group = "protobuf"
//
//        from(layout.projectDirectory.dir("build/hedera-protobufs/block"))
//        into(layout.buildDirectory.dir("block-node-protobuf/block"))
//        mustRunAfter(tasks.generateProto)
//    }
//
//    tasks.register<Copy>("exportConsensusNodePlatformProto") {
//        description =
//            "Copies the protobuf files from block api to a combined CN & BN block-streams related
// protobuf directory"
//        group = "protobuf"
//
//        from(layout.projectDirectory.dir("build/hedera-protobufs/platform"))
//        into(layout.buildDirectory.dir("block-node-protobuf/platform"))
//        mustRunAfter(tasks.generateProto)
//    }
//
//    tasks.register<Copy>("exportConsensusNodeServicesProto") {
//        description =
//            "Copies the protobuf files from block api to a combined CN & BN block-streams related
// protobuf directory"
//        group = "protobuf"
//
//        from(layout.projectDirectory.dir("build/hedera-protobufs/services"))
//        into(layout.buildDirectory.dir("block-node-protobuf/services"))
//        mustRunAfter(tasks.generateProto)
//    }
//
//    tasks.register<Copy>("exportConsensusNodeStreamsProto") {
//        description =
//            "Copies the protobuf files from block api to a combined CN & BN block-streams related
// protobuf directory"
//        group = "protobuf"
//
//        from(layout.projectDirectory.dir("build/hedera-protobufs/streams"))
//        into(layout.buildDirectory.dir("block-node-protobuf/streams"))
//        mustRunAfter(tasks.generateProto)
//    }
// }
//
// val exportBlockNodeApiProto: TaskProvider<Copy> =
//    tasks.register<Copy>("exportBlockNodeApiProto") {
//        description =
//            "Copies the protobuf files from block api to a combined CN & BN block-streams related
// protobuf directory"
//        group = "protobuf"
//
//        from(layout.projectDirectory.dir("src/main/proto/org/hiero/block/api"))
//        into(layout.buildDirectory.dir("block-node-protobuf"))
//        include("block/**.proto")
//        include("platform/**.proto")
//        include("services/**.proto")
//        include("streams/**.proto")
//        mustRunAfter(tasks.generateProto)
//    }

sourceSets {
    main {
        pbj { srcDir(cloneHederaProtobufs.map { it.localCloneDirectory }) }
        proto { srcDir(cloneHederaProtobufs.map { it.localCloneDirectory }) }
    }
}

// sourceSets {
//    main {
//        pbj {
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("services") })
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("block") })
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("platform") })
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("streams") })
//        }
//        proto {
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("services") })
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("block") })
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("platform") })
//            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("streams") })
//        }
//    }
// }

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
