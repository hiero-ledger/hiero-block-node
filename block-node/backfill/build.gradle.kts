// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("com.hedera.pbj.pbj-compiler") version "0.11.13"
}

description = "Hiero Block Node Backfill Plugin"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

tasks.javadoc {
    options {
        this as StandardJavadocDocletOptions
        // There are violations in the generated pbj code
        addStringOption("Xdoclint:-reference,-html", "-quiet")
    }
}

pbj { generateTestClasses = false }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
    runtimeOnly("io.helidon.logging.jul")
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.hiero.block.node.app.test.fixtures")
}

sourceSets {
    main {
        pbj {
            srcDir(
                layout.projectDirectory.dir(
                    "src/main/java/org/hiero/block/node/backfill/client/proto"
                )
            )
        }
    }
}
