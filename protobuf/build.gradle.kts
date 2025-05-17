// SPDX-License-Identifier: Apache-2.0
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

val generateBlockNodeProtoArtifact: TaskProvider<Exec> =
    tasks.register<Exec>("generateBlockNodeProtoArtifact") {
        description =
            "Retrieves CN protobuf and combines it along with local BN protobuf into a single artifact"
        group = "protobuf"

        workingDir(layout.projectDirectory)
        val cnTagHash = "efb0134e921b32ed6302da9c93874d65492e876f" // v0.62.2
        commandLine(
            "sh",
            "-c",
            "./scripts/build-bn-proto.sh -t $cnTagHash -v ${project.version} -o block-node-protobuf",
        )
    }

sourceSets {
    main {
        pbj {
            srcDir(
                generateBlockNodeProtoArtifact.map {
                    "${layout.projectDirectory}/block-node-protobuf"
                }
            )
        }
        proto {
            srcDir(
                generateBlockNodeProtoArtifact.map {
                    "${layout.projectDirectory}/block-node-protobuf"
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
