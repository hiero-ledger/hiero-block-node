// SPDX-License-Identifier: Apache-2.0
dependencies {
    api(platform("io.netty:netty-bom:4.2.15.Final"))
    api(platform("com.google.cloud:libraries-bom:26.85.0"))
}

dependencies.constraints {
    val daggerVersion = "2.60"
    val grpcIoVersion = "1.82.2"
    val hederaCryptographyVersion = "3.11.2"
    val helidonVersion = "4.5.0"
    val pbjVersion = pluginVersions.version("com.hedera.pbj.pbj-compiler")
    val protobufVersion = "4.35.1"
    val hederaVersion = "0.76.0-rc.1"
    val eclipseCollectionsVersion = "13.0.0"
    val mockitoVersion = "5.23.0"
    val testContainersVersion = "1.21.4"
    val buckyVersion = "0.1.0"
    val s3MockVersion = "4.11.0"
    val jUnitVersion = "6.1.1"

    api("com.github.luben:zstd-jni:1.5.7-11") { because("com.github.luben.zstd_jni") }
    api("com.github.spotbugs:spotbugs-annotations:4.10.2") {
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
    api("com.hedera.hashgraph:swirlds-state-api:$hederaVersion") {
        because("com.swirlds.state.api")
    }
    api("com.hedera.hashgraph:swirlds-state-impl:$hederaVersion") {
        because("com.swirlds.state.impl")
    }
    api("com.hedera.hashgraph:swirlds-virtualmap:$hederaVersion") {
        because("com.swirlds.virtualmap")
    }
    api("com.hedera.hashgraph:swirlds-common:$hederaVersion") { because("com.swirlds.common") }
    api("com.hedera.hashgraph:swirlds-metrics-api:$hederaVersion") {
        because("com.swirlds.metrics.api")
    }
    api("com.hedera.hashgraph:swirlds-metrics-impl:$hederaVersion") {
        because("com.swirlds.metrics.impl")
    }
    api("com.hedera.hashgraph:swirlds-merkledb:$hederaVersion") { because("com.swirlds.merkledb") }
    api("com.hedera.hashgraph:swirlds-logging:$hederaVersion") { because("com.swirlds.logging") }
    api("com.hedera.hashgraph:base-concurrent:$hederaVersion") {
        because("org.hiero.base.concurrent")
    }
    api("com.hedera.hashgraph:base-crypto:$hederaVersion") { because("org.hiero.base.crypto") }
    api("com.hedera.hashgraph:base-utility:$hederaVersion") { because("org.hiero.base.utility") }
    api("com.hedera.hashgraph:consensus-model:$hederaVersion") {
        because("org.hiero.consensus.model")
    }
    api("com.hedera.hashgraph:consensus-utility:$hederaVersion") {
        because("org.hiero.consensus.utility")
    }
    api("com.hedera.hashgraph:consensus-metrics:$hederaVersion") {
        because("org.hiero.consensus.metrics")
    }
    api("com.hedera.hashgraph:consensus-concurrent:$hederaVersion") {
        because("org.hiero.consensus.concurrent")
    }
    api("com.hedera.hashgraph:consensus-reconnect:$hederaVersion") {
        because("org.hiero.consensus.reconnect")
    }
    api("com.goterl:lazysodium-java:5.2.0") { because("com.goterl.lazysodium") }
    api("com.goterl:resource-loader:2.1.0") { because("com.goterl.resourceloader") }
    api("net.java.dev.jna:jna:5.18.1") { because("com.sun.jna") }
    api("org.json:json:20250517") { because("org.json") }
    api("io.prometheus:simpleclient:0.16.0") { because("simpleclient") }
    api("io.prometheus:simpleclient_common:0.16.0") { because("simpleclient.common") }
    api("io.prometheus:simpleclient_httpserver:0.16.0") { because("simpleclient.httpserver") }
    api("io.prometheus:simpleclient_tracer_common:0.16.0") { because("simpleclient.tracer.common") }
    api("org.hyperledger.besu:besu-native-common:1.3.0") {
        because("org.hyperledger.besu.nativelib.common")
    }
    api("org.hyperledger.besu:secp256k1:1.3.0") {
        because("org.hyperledger.besu.nativelib.secp256k1")
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
    api("org.junit.jupiter:junit-jupiter-api:${jUnitVersion}") { because("org.junit.jupiter.api") }
    api("org.junit.jupiter:junit-jupiter-engine:${jUnitVersion}") {
        because("org.junit.jupiter.engine")
    }
    api("org.junit.platform:junit-platform-launcher:${jUnitVersion}") {
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
    api("com.adobe.testing:s3mock-testcontainers:${s3MockVersion}") {
        because("s3mock.testcontainers")
    }

    // Versions of additional tools that are not part of the product or test module paths
    api("com.google.protobuf:protoc:${protobufVersion}")
    tasks.checkVersionConsistency { excludes.add("com.google.protobuf:protoc") }
}
