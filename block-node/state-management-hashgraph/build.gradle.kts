// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Hiero Block Node State Management Hashgraph Plugin (beta)"

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-Xlint:-exports") }

// Workaround (hiero-gradle-conventions 0.7.10): the palantir `spotlessJava` formatter and the
// custom `spotlessJavaInfoFiles` formatter are BOTH applied to `module-info.java` and disagree on
// the blank line before `provides`, so no form satisfies both once the file differs from
// origin/main (spotless ratchet). module-info is meant to be owned by `spotlessJavaInfoFiles`;
// exclude it from the palantir formatter here to restore that. Remove once the convention excludes
// module-info from the java formatter upstream.
spotless { java { targetExclude("**/module-info.java", "**/package-info.java") } }

mainModuleInfo {
    runtimeOnly("com.hedera.pbj.grpc.helidon.config")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("io.helidon.logging.jul")
}

testModuleInfo {
    requires("org.hiero.block.node.app.test.fixtures")
    requires("io.helidon.webserver")
    requires("org.assertj.core")
    requires("org.junit.jupiter.api")

    runtimeOnly("com.swirlds.config.extensions")
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.hiero.metrics")
}
