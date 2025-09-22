// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.base.lifecycle")
    id("org.hiero.gradle.base.jpms-modules")
    id("org.hiero.gradle.check.spotless")
    id("org.hiero.gradle.check.spotless-kotlin")
}

dependencies {
    api(platform("io.netty:netty-bom:4.2.6.Final"))
    api(platform("com.google.cloud:libraries-bom:26.68.0"))
}

dependencies.constraints {
    val daggerVersion = "2.56.2"
    val grpcIoVersion = "1.75.0"
    val helidonVersion = "4.2.6"
    // When Upgrading pbjVersion, also need to update pbjCompiler version on
    // block-node/protobuf-pbj/build.gradle.kts
    val pbjVersion = "0.11.15"
    val protobufVersion = "4.32.0"
    val swirldsVersion = "0.61.3"
    val mockitoVersion = "5.19.0"
    val testContainersVersion = "1.21.3"

    api("com.github.luben:zstd-jni:1.5.7-4") { because("com.github.luben.zstd_jni") }
    api("com.github.spotbugs:spotbugs-annotations:4.9.5") {
        because("com.github.spotbugs.annotations")
    }
    api("com.google.auto.service:auto-service-annotations:1.1.1") {
        because("com.google.auto.service")
    }
    api("com.google.guava:guava:33.5.0-jre") { because("com.google.common") }
    api("com.google.protobuf:protobuf-java-util:$protobufVersion") {
        because("com.google.protobuf.util")
    }
    api("com.google.protobuf:protoc:$protobufVersion") { because("google.proto") }
    api("com.hedera.pbj:pbj-grpc-client-helidon:${pbjVersion}") {
        because("com.hedera.pbj.grpc.client.helidon")
    }
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

    api("io.helidon.webclient:helidon-webclient-grpc:$helidonVersion") {
        because("io.helidon.webclient.grpc")
    }
    api("io.helidon.webclient:helidon-webclient:$helidonVersion") {
        because("io.helidon.webclient")
    }
    api("org.jetbrains:annotations:26.0.2") { because("org.jetbrains.annotations") }

    // gRPC dependencies
    api("io.grpc:grpc-api:$grpcIoVersion") { because("io.grpc") }
    api("io.grpc:grpc-stub:$grpcIoVersion") { because("io.grpc.stub") }
    api("io.grpc:grpc-protobuf:$grpcIoVersion") { because("io.grpc.protobuf") }
    api("io.grpc:grpc-netty:$grpcIoVersion") { because("io.grpc.netty") }

    // command line tool
    api("info.picocli:picocli:4.7.7") { because("info.picocli") }

    // needed for dagger
    api("com.google.dagger:dagger:$daggerVersion") { because("dagger") }
    api("com.google.dagger:dagger-compiler:$daggerVersion") { because("dagger.compiler") }

    // Testing only versions
    api("com.github.docker-java:docker-java-api:3.6.0") { because("com.github.dockerjava.api") }
    api("org.assertj:assertj-core:3.27.4") { because("org.assertj.core") }
    api("org.junit.jupiter:junit-jupiter-api:5.13.4") { because("org.junit.jupiter.api") }
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
    api("com.google.jimfs:jimfs:1.3.1") { because("com.google.common.jimfs") }
    api("io.minio:minio:8.5.17") { because("io.minio") }
    api("com.squareup.okio:okio-jvm:3.16.0") { because("okio") } // required by minio
}
