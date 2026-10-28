// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.protobuf")
}

description = "Hiero Block Node Protobuf Protoc API"

sourceSets {
    main {
        proto {
            // use sources from 'protobuf' module
            srcDir(layout.projectDirectory.dir("../../protobuf-sources/src/main/proto"))
            // use sources from CN repository cloned by 'protobuf' module (see task dependency)
            srcDir(layout.projectDirectory.dir("../../protobuf-sources/block-node-protobuf"))
            // exclude BN files at root level
            exclude("*.proto")
        }
    }
}

// jjohannes: remove cross-project task dependency once the following issue is addressed
// https://github.com/hiero-ledger/hiero-gradle-conventions/issues/185
tasks.generateProto { dependsOn(":protobuf-sources:generateBlockNodeProtoArtifact") }

tasks.processProtoResources { dependsOn(":protobuf-sources:generateBlockNodeProtoArtifact") }

// The generated protoc API refers to types in packages this module does not export, for example
// TransactionBody refers to the CLPR transaction types. Instead of exporting every such package,
// the 'exports' lint check is turned off for the generated code, as in the PBJ module.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }
