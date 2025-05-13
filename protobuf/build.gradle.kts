// SPDX-License-Identifier: Apache-2.0
import org.gradle.kotlin.dsl.assign

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

val extractProtoFromConsensusNodeProtoJar: TaskProvider<Copy> =
    tasks.register<Copy>("extractProtoFromConsensusNodeProtoJar") {
        description =
            "Copies the protobuf files from the hedera-protobuf-java-api jar to the build root directory 2"
        group = "protobuf"

        val protoJar =
            configurations.runtimeClasspath.get().single {
                it.name.contains("hedera-protobuf-java-api")
            }
        from(zipTree(protoJar))
        include(
            "block/**/*.proto",
            "platform/**/*.proto",
            "services/**/*.proto",
            "streams/**/*.proto",
        )
        into(layout.buildDirectory.dir("block-streams-protobuf"))
    }

val exportBlockNodeApiProto: TaskProvider<Copy> =
    tasks.register<Copy>("exportBlockNodeApiProto") {
        description = "Copies the protobuf files from block api to the extracted directory"
        group = "protobuf"

        from(layout.projectDirectory.dir("src/main/proto/org/hiero/block/api"))
        into(layout.buildDirectory.dir("block-streams-protobuf"))
        mustRunAfter(tasks.generateProto)
    }

sourceSets {
    main {
        pbj { srcDir(extractProtoFromConsensusNodeProtoJar.map { it.destinationDir.path }) }
        proto { srcDir(extractProtoFromConsensusNodeProtoJar.map { it.destinationDir.path }) }
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
