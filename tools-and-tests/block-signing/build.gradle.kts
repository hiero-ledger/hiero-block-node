// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description =
    "Test-only block-proof signing library (RSA WRB proofs and TSS/hinTS block signatures)."

// Remove the following line to enable all 'javac' lint checks that we have turned on by default
// and then fix the reported issues.
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

testModuleInfo {
    requires("org.assertj.core")
    requires("org.junit.jupiter.api")
}

// Headroom for the offline WRAPS proof regeneration (WrapsProofGeneratorTest), which loads a large
// proving key and runs a SNARK. Normal tests use only a fraction of this.
tasks.test { maxHeapSize = "6g" }
