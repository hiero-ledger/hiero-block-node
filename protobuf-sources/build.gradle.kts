// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Protobuf Sources"

// Add probuf source files as resources to have them packaged in Jar
sourceSets.main { resources { srcDir(layout.projectDirectory.dir("src/main/proto")) } }

// Skip javadoc generation for this module as it only contains protobuf sources
tasks.javadoc { enabled = false }

val generateBlockNodeProtoArtifact: TaskProvider<Exec> =
    tasks.register<Exec>("generateBlockNodeProtoArtifact") {
        description =
            "Retrieves CN protobuf and combines it along with local BN protobuf into a single artifact"
        group = "protobuf"

        workingDir(layout.projectDirectory)
        // When upgrading the CN proto version, always check that the file:
        // proto/internal/unparsed.proto matches structure for
        // block-node-protobuf/block/stream/block_item.proto after update otherwise Verification
        // might fail
        val cnTagHash = "v0.71.0-alpha.1"

        // run build-bn-proto.sh skipping inclusion of BN API as it messes up proto considerations
        commandLine(
            "sh",
            "-c",
            "${layout.projectDirectory}/scripts/build-bn-proto.sh -t $cnTagHash -v ${project.version} -o ${layout.projectDirectory}/block-node-protobuf -i true -b ${layout.projectDirectory}/src/main/proto/",
        )
    }

val cleanUpAfterBlockNodeProtoArtifact: TaskProvider<Exec> =
    tasks.register<Exec>("cleanUpAfterBlockNodeProtoArtifact") {
        description = "Cleans up left over files from generateBlockNodeProtoArtifact task"
        group = "protobuf"

        workingDir(layout.projectDirectory)

        // clean up intermediate files generated from build-bn-proto.sh run
        commandLine(
            "rm",
            "-rf",
            "${layout.projectDirectory}/block-node-protobuf",
            "&&",
            "rm",
            "-rf",
            "${layout.projectDirectory}/hiero-consensus-node",
        )
    }

// further clean up of the project downloaded files
tasks.named<Delete>("clean") {
    // remove the downloaded block-node-protobuf directory and hiero-consensus-node directory
    delete(
        layout.projectDirectory.dir("block-node-protobuf"),
        layout.projectDirectory.dir("hiero-consensus-node"),
    )
    // remove the downloaded tarball
    delete(project.fileTree(layout.projectDirectory) { include("block-node-protobuf-*.tgz") })
}
