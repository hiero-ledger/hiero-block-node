// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.base.lifecycle")
    id("org.hiero.gradle.base.jpms-modules")
    id("org.hiero.gradle.check.spotless")
    id("org.hiero.gradle.check.spotless-kotlin")
}

dependencies { api(platform("com.google.cloud:libraries-bom:26.55.0")) }

dependencies.constraints {
    val daggerVersion = "2.56"
    val grpcIoVersion = "1.71.0"
    val helidonVersion = "4.1.6"
    val pbjVersion = "0.9.17"
    val protobufVersion = "4.30.0"
    val swirldsVersion = "0.59.2"
    val mockitoVersion = "5.16.1"
    val testContainersVersion = "1.20.6"

    api("com.github.luben:zstd-jni:1.5.7-1") { because("com.github.luben.zstd_jni") }
    api("com.github.spotbugs:spotbugs-annotations:4.9.2") {
        because("com.github.spotbugs.annotations")
    }
    api("com.google.auto.service:auto-service-annotations:1.1.1") {
        because("com.google.auto.service")
    }
    api("com.google.guava:guava:33.4.0-jre") { because("com.google.common") }
    api("com.google.j2objc:j2objc-annotations:3.0.0") { because("com.google.j2objc.annotations") }
    api("com.google.protobuf:protobuf-java-util:$protobufVersion") {
        because("com.google.protobuf.util")
    }
    api("com.google.protobuf:protoc:$protobufVersion") { because("google.proto") }
    api("com.hedera.pbj:pbj-grpc-helidon:${pbjVersion}") { because("com.hedera.pbj.grpc.helidon") }
    api("com.hedera.pbj:pbj-grpc-helidon-config:${pbjVersion}") {
        because("com.hedera.pbj.grpc.helidon.config")
    }
    api("com.hedera.pbj:pbj-runtime:${pbjVersion}") { because("com.hedera.pbj.runtime") }
    api("com.lmax:disruptor:4.0.0") { because("com.lmax.disruptor") }
    api("com.swirlds:swirlds-common:$swirldsVersion") { because("com.swirlds.common") }
    api("com.swirlds:swirlds-config-impl:$swirldsVersion") { because("com.swirlds.config.impl") }
    api("io.helidon.logging:helidon-logging-jul:$helidonVersion") {
        because("io.helidon.logging.jul")
    }
    api("io.helidon.webserver:helidon-webserver-grpc:$helidonVersion") {
        because("io.helidon.webserver.grpc")
    }
    api("io.helidon.webserver:helidon-webserver:$helidonVersion") {
        because("io.helidon.webserver")
    }
    api("org.jetbrains:annotations:26.0.2") { because("org.jetbrains.annotations") }

    // gRPC dependencies
    api("io.grpc:grpc-api:$grpcIoVersion") { because("io.grpc") }
    api("io.grpc:grpc-stub:$grpcIoVersion") { because("io.grpc.stub") }
    api("io.grpc:grpc-protobuf:$grpcIoVersion") { because("io.grpc.protobuf") }
    api("io.grpc:grpc-netty:$grpcIoVersion") { because("io.grpc.netty") }
    api("io.grpc:protoc-gen-grpc-java:1.71.0")

    // command line tool
    api("info.picocli:picocli:4.7.6") { because("info.picocli") }

    // needed for dagger
    api("com.google.dagger:dagger:$daggerVersion") { because("dagger") }
    api("com.google.dagger:dagger-compiler:$daggerVersion") { because("dagger.compiler") }

    // Testing only versions
    api("com.github.docker-java:docker-java-api:3.4.1") { because("com.github.dockerjava.api") }
    api("io.github.cdimascio:dotenv-java:3.2.0") { because("io.github.cdimascio.dotenv.java") }
    api("org.assertj:assertj-core:3.27.3") { because("org.assertj.core") }
    api("org.junit.jupiter:junit-jupiter-api:5.11.4") { because("org.junit.jupiter.api") }
    api("org.mockito:mockito-core:${mockitoVersion}") { because("org.mockito") }
    api("org.mockito:mockito-junit-jupiter:${mockitoVersion}") {
        because("org.mockito.junit.jupiter")
    }
    api("org.testcontainers:junit-jupiter:${testContainersVersion}") {
        because("org.testcontainers.junit.jupiter")
    }
    api("org.testcontainers:testcontainers:${testContainersVersion}") {
        because("org.testcontainers")
    }

    api("com.google.auto.service:auto-service:1.1.1") {
        because("com.google.auto.service.processor")
    }
}
