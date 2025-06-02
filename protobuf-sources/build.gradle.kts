// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node Protobuf Sources"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-exports,-deprecation,-removal,-dep-ann")
}

// Skip javadoc generation for this module as it only contains protobuf sources
tasks.javadoc { enabled = false }

val generateBlockNodeProtoArtifact: TaskProvider<Exec> =
    tasks.register<Exec>("generateBlockNodeProtoArtifact") {
        description =
            "Retrieves CN protobuf and combines it along with local BN protobuf into a single artifact"
        group = "protobuf"

        workingDir(layout.projectDirectory)
        val cnTagHash = "efb0134e921b32ed6302da9c93874d65492e876f" // v0.62.2

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
