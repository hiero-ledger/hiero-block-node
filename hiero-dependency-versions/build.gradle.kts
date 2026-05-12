// SPDX-License-Identifier: Apache-2.0
dependencies {
    api(platform("io.netty:netty-bom:4.2.12.Final"))
    api(platform("com.google.cloud:libraries-bom:26.80.0"))
}

dependencies.constraints {
    val daggerVersion = "2.59.2"
    val grpcIoVersion = "1.81.0"
    val hederaCryptographyVersion = "3.8.1"
    val helidonVersion = "4.4.1"
    val pbjVersion = pluginVersions.version("com.hedera.pbj.pbj-compiler")
    val protobufVersion = "4.34.1"
    val hederaVersion = "0.74.0-rc.1"
    val eclipseCollectionsVersion = "13.0.0"
    val mockitoVersion = "5.23.0"
    val testContainersVersion = "1.21.4"
    val buckyVersion = "0.1.0-rc4"
    val s3MockVersion = "4.11.0"

    api("com.github.luben:zstd-jni:1.5.7-8") { because("com.github.luben.zstd_jni") }
    api("com.github.spotbugs:spotbugs-annotations:4.9.8") {
        because("com.github.spotbugs.annotations")
    }
    api("com.google.auto.service:auto-service-annotations:1.1.1") {
        because("com.google.auto.service")
    }
    api("com.google.guava:guava:33.6.0-jre") { because("com.google.common") }
    api("com.google.protobuf:protobuf-java-util:$protobufVersion") {
        because("com.google.protobuf.util")
    }
    api("com.hedera.pbj:pbj-grpc-client-helidon:${pbjVersion}") {
        because("com.hedera.pbj.grpc.client.helidon")
    }
    api("com.hedera.pbj:pbj-grpc-helidon:${pbjVersion}") { because("com.hedera.pbj.grpc.helidon") }
    api("com.hedera.pbj:pbj-grpc-helidon-config:${pbjVersion}") {
        because("com.hedera.pbj.grpc.helidon.config")
    }
    api("com.hedera.pbj:pbj-runtime:${pbjVersion}") { because("com.hedera.pbj.runtime") }
    api("com.lmax:disruptor:4.0.0") { because("com.lmax.disruptor") }
    api("com.hedera.hashgraph:swirlds-base:$hederaVersion") { because("com.swirlds.base") }
    api("com.hedera.hashgraph:swirlds-config-api:$hederaVersion") {
        because("com.swirlds.config.api")
    }
    api("com.hedera.hashgraph:swirlds-config-extensions:$hederaVersion") {
        because("com.swirlds.config.extensions")
    }
    api("com.hedera.hashgraph:swirlds-config-impl:$hederaVersion") {
        because("com.swirlds.config.impl")
    }
    api("com.hedera.hashgraph:hiero-metrics:$hederaVersion") { because("org.hiero.metrics") }
    api("com.hedera.hashgraph:openmetrics-httpserver:$hederaVersion") {
        because("org.hiero.metrics.openmetrics.httpserver")
    }
    api("com.hedera.cryptography:hedera-cryptography-wraps:$hederaCryptographyVersion") {
        because("com.hedera.cryptography.wraps")
    }
    api("com.hedera.common:hedera-common-nativesupport:$hederaCryptographyVersion") {
        because("com.hedera.common.nativesupport")
    }
    api("io.helidon.logging:helidon-logging-jul:$helidonVersion") {
        because("io.helidon.logging.jul")
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
    api("org.jetbrains:annotations:26.1.0") { because("org.jetbrains.annotations") }

    // gRPC dependencies
    api("io.grpc:grpc-api:$grpcIoVersion") { because("io.grpc") }
    api("io.grpc:grpc-stub:$grpcIoVersion") { because("io.grpc.stub") }
    api("io.grpc:grpc-protobuf:$grpcIoVersion") { because("io.grpc.protobuf") }
    api("io.grpc:grpc-netty:$grpcIoVersion") { because("io.grpc.netty") }

    // Eclipse Collections (primitive collections)
    api("org.eclipse.collections:eclipse-collections-api:$eclipseCollectionsVersion") {
        because("org.eclipse.collections.api")
    }
    api("org.eclipse.collections:eclipse-collections:$eclipseCollectionsVersion") {
        because("org.eclipse.collections.impl")
    }

    // command line tool
    api("info.picocli:picocli:4.7.7") { because("info.picocli") }

    // needed for dagger
    api("com.google.dagger:dagger:$daggerVersion") { because("dagger") }
    api("com.google.dagger:dagger-compiler:$daggerVersion") { because("dagger.compiler") }

    // Testing only versions
    api("com.github.docker-java:docker-java-api:3.7.1") { because("com.github.dockerjava.api") }
    api("org.assertj:assertj-core:3.27.7") { because("org.assertj.core") }
    api("org.junit.jupiter:junit-jupiter-api:6.0.3") { because("org.junit.jupiter.api") }
    api("org.junit.jupiter:junit-jupiter-engine:6.0.3") { because("org.junit.jupiter.engine") }
    api("org.junit.platform:junit-platform-launcher:6.0.3") {
        because("org.junit.platform.launcher")
    }
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
    api("com.google.jimfs:jimfs:1.3.1") { because("com.google.common.jimfs") }
    api("com.hedera.bucky:bucky-client:${buckyVersion}") { because("com.hedera.bucky") }
    api("io.minio:minio:8.5.17") { because("io.minio") }
    api("com.squareup.okio:okio-jvm:3.17.0") { because("okio") } // required by minio
    api("com.squareup.okio:okio:3.17.0") {
        because("okio")
    } // override strict 3.6.0 from bucky-client (broken JPMS module descriptor)
    api("com.adobe.testing:s3mock-testcontainers:${s3MockVersion}") {
        because("s3mock.testcontainers")
    }

    // Versions of additional tools that are not part of the product or test module paths
    api("com.google.protobuf:protoc:${protobufVersion}")
    tasks.checkVersionConsistency { excludes.add("com.google.protobuf:protoc") }
}
