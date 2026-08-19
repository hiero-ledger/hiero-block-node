// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
    id("com.hedera.pbj.pbj-compiler")
}

description = "Hiero Block Node Verification Service"

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

// The 'pbj' compiler is applied only to compile the test fixture proto definitions in
// 'src/testFixtures/proto' used by the forward compatibility tests. No production proto
// sources exist in this module.
pbj { generateTestClasses = false }

mainModuleInfo {
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")
    requires("org.hiero.block.node.block.verification.test.fixtures")
    requires("org.hiero.block.signing")
    requires("com.google.common.jimfs")
    requires("org.assertj.core")
    requires("org.junit.jupiter.api")
    requires("org.junit.jupiter.params")
}

/// Materializes a chain of harness-signed .blk.gz files. Invoked by the E2E lifecycle
/// workflow to produce valid TSS-signed blocks at CI time instead of reading a committed
/// fixture set. Usage:
///   ./gradlew :block-verification:generateHarnessBlocks -PoutputDir=/tmp/blocks -Pcount=5
tasks.register<JavaExec>("generateHarnessBlocks") {
    description = "Generate a chain of TSS-signed .blk.gz files via HarnessChainBuilder."
    group = "verification"
    dependsOn(tasks.named("testClasses"))
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("org.hiero.block.node.block.verification.harness.GenerateHarnessBlocksMain")
    val outputDir = (project.findProperty("outputDir") as String?) ?: "build/harness-blocks"
    val count = (project.findProperty("count") as String?) ?: "5"
    args(outputDir, count)
}
