// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    // When upgrading pbjVersion, also need to update pbjVersion on
    // hiero-dependency-versions/build.gradle.kts
    id("com.hedera.pbj.pbj-compiler") version "0.11.13"
}

description = "Hiero Block Node Protobuf PBJ API"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-exports,-deprecation,-removal,-dep-ann")
}

tasks.javadoc {
    options {
        this as StandardJavadocDocletOptions
        // There are violations in the generated pbj code
        addStringOption("Xdoclint:-reference,-html", "-quiet")
    }
}

pbj { generateTestClasses = false }

sourceSets {
    main {
        pbj {
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
tasks.generatePbjSource { dependsOn(":block-node-protobuf-sources:generateBlockNodeProtoArtifact") }

tasks.test {
    // we can exclude the standard protobuf generated tests as we don't need to test them again here
    // this speeds up the block node project test run no end :-)
    exclude("**com/hedera/**")
}
