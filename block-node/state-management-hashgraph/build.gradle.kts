// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node State Management Hashgraph Plugin (beta)"

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

// swirlds-state-impl/consensus-model/consensus-roster require com.hedera.node.hapi; exclude it
// so our own protobuf-pbj's packages don't collide. Scoped here since no other module needs it.
configurations.all { exclude(group = "com.hedera.hashgraph", module = "hapi") }

// Workaround (hiero-gradle-conventions 0.7.10): the palantir `spotlessJava` formatter and the
// custom `spotlessJavaInfoFiles` formatter are BOTH applied to `module-info.java` and disagree on
// the blank line before `provides`, so no form satisfies both once the file differs from
// origin/main (spotless ratchet). module-info is meant to be owned by `spotlessJavaInfoFiles`;
// exclude it from the palantir formatter here to restore that. Remove once the convention excludes
// module-info from the java formatter upstream.
spotless { java { targetExclude("**/module-info.java", "**/package-info.java") } }

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")
    requires("java.logging")
    requires("org.assertj.core")
    requires("org.junit.jupiter.api")

    runtimeOnly("com.swirlds.config.extensions")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.hiero.metrics")
}
