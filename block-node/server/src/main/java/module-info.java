// SPDX-License-Identifier: Apache-2.0
import com.hedera.block.server.config.BlockNodeConfigExtension;

module com.hedera.block.server {
    exports com.hedera.block.server;
    exports com.hedera.block.server.consumer;
    exports com.hedera.block.server.exception;
    exports com.hedera.block.server.persistence.storage;
    exports com.hedera.block.server.persistence.storage.archive;
    exports com.hedera.block.server.persistence.storage.compression;
    exports com.hedera.block.server.persistence.storage.path;
    exports com.hedera.block.server.persistence.storage.write;
    exports com.hedera.block.server.persistence.storage.read;
    exports com.hedera.block.server.persistence.storage.remove;
    exports com.hedera.block.server.config;
    exports com.hedera.block.server.config.logging;
    exports com.hedera.block.server.mediator;
    exports com.hedera.block.server.metrics;
    exports com.hedera.block.server.events;
    exports com.hedera.block.server.health;
    exports com.hedera.block.server.ack;
    exports com.hedera.block.server.persistence;
    exports com.hedera.block.server.notifier;
    exports com.hedera.block.server.service;
    exports com.hedera.block.server.pbj;
    exports com.hedera.block.server.producer;
    exports com.hedera.block.server.verification;
    exports com.hedera.block.server.verification.session;
    exports com.hedera.block.server.verification.signature;
    exports com.hedera.block.server.verification.service;
    exports com.hedera.block.server.block;

    requires com.hedera.block.common;
    requires com.hedera.block.stream;
    requires com.hedera.pbj.grpc.helidon.config;
    requires com.hedera.pbj.grpc.helidon;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires com.swirlds.metrics.api;
    requires com.github.luben.zstd_jni;
    requires com.lmax.disruptor;
    requires dagger;
    requires io.helidon.common;
    requires io.helidon.webserver;
    requires javax.inject;
    requires static transitive com.github.spotbugs.annotations;
    requires static transitive com.google.auto.service;
    requires static java.compiler; // javax.annotation.processing.Generated

    provides com.swirlds.config.api.ConfigurationExtension with
            BlockNodeConfigExtension;
}
