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
    requires("org.junit.jupiter.params")
}

// The build resolves the TSS library through its 'verifier' variant
// (gradle/modules.properties), which leaves out the parameters needed to construct a
// WRAPS. Only the opt-in WrapsProofGeneratorTest constructs one, so when
// GENERATE_WRAPS_PROOF=true the test run swaps the verifier jar for the full jar of the
// same version (same module, same classes).
val wrapsProver =
    configurations.dependencyScope("wrapsProver") {
        description =
            "The full TSS library jar, used only to regenerate the committed genesis WRAPS"
    }
val wrapsProverClasspath =
    configurations.resolvable("wrapsProverClasspath") {
        extendsFrom(wrapsProver.get())
        shouldResolveConsistentlyWith(configurations.testRuntimeClasspath.get())
        attributes {
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
            attribute(
                LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                objects.named(LibraryElements.JAR),
            )
        }
    }

dependencies { "wrapsProver"("com.hedera.cryptography:hedera-cryptography-wraps") }

val generateWrapsProof = providers.environmentVariable("GENERATE_WRAPS_PROOF").map { it == "true" }

tasks.test {
    if (generateWrapsProof.getOrElse(false)) {
        val isWrapsJar = { file: File -> file.name.startsWith("hedera-cryptography-wraps-") }
        classpath =
            classpath.filter { !isWrapsJar(it) } +
                wrapsProverClasspath.get().filter { isWrapsJar(it) }
    }
}
