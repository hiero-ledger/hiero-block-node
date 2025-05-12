// SPDX-License-Identifier: Apache-2.0
import org.hiero.gradle.tasks.GitClone

plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.protobuf")
    // When upgrading pbjVersion, also need to update pbjVersion on
    // hiero-dependency-versions/build.gradle.kts
    id("com.hedera.pbj.pbj-compiler") version "0.11.3"
}

dependencies { api("com.hedera.hashgraph:hedera-protobuf-java-api:0.62.1") }

description = "Hiero Block Node Protobuf API"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-exports,-deprecation,-removal,-dep-ann")
}

// Add downloaded HAPI repo protobuf files into build directory and add to sources to build them
val cloneHederaProtobufs =
    tasks.register<GitClone>("cloneHederaProtobufs") {
        url = "https://github.com/hashgraph/hedera-protobufs.git"
        localCloneDirectory = layout.buildDirectory.dir("hedera-protobufs")

        // uncomment below to use a specific tag
        // tag = "v0.53.0" or a specific commit like "0047255"
        tag = "c71e879a03f713961f52fb774e706e38c5e9d48a"

        // uncomment below to use a specific branch
        // branch = "main"

        // remove the block_service.proto file pulled from hedera-protobufs in favour of local
        // version
        doLast { localCloneDirectory.file("block/block_service.proto").get().asFile.delete() }
//        dependsOn(unpackConsensusNodeProtoJar)
    }

val unpackConsensusNodeProtoJar: TaskProvider<Copy> =
    tasks.register<Copy>("unpackConsensusNodeProtoJar") {
        description = "Copies the protobuf files from the hedera-protobuf-java-api jar to the build root directory"
        group = "protobuf"

        val protoJar = configurations.runtimeClasspath.get().single { it.name.contains("hedera-protobuf-java-api") }
        from(zipTree(protoJar))
//        into(layout.buildDirectory.dir("consensus-node-protobuf"))
        into(layout.buildDirectory.dir("resources/main"))
//        into(layout.projectDirectory.dir("src/main/proto/com/hedera/hapi"))
    }

val copyBlockApiProto: TaskProvider<Copy> =
    tasks.register<Copy>("copyBlockApiProto") {
        description = "Copies the protobuf files from block api to the extracted directory"
        group = "protobuf"

        from(layout.projectDirectory.dir("src/main/proto/org/hiero/block/api"))
//        into(layout.buildDirectory.dir("extracted-include-protos/main/block-node-api"))
        into(layout.buildDirectory.dir("resources/main"))

}

sourceSets {
    main {
        pbj {
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("services") })
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("block") })
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("platform") })
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("streams") })
//            srcDir("${layout.buildDirectory}/consensus-node-protobufe/block")
//            srcDir("${layout.buildDirectory}/resources/main")
        }
        proto {
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("services") })
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("block") })
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("platform") })
            srcDir(cloneHederaProtobufs.flatMap { it.localCloneDirectory.dir("streams") })
//            srcDir("src/main/proto")
//            srcDir("${layout.buildDirectory}/consensus-node-protobuf/block")
//            srcDir("${layout.buildDirectory}/resources/main")
        }
    }
}

tasks.named("compileJava") {
    dependsOn(unpackConsensusNodeProtoJar)
    dependsOn(copyBlockApiProto)
}

tasks.named("copyBlockApiProto") {
    mustRunAfter(tasks.generateProto)
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
