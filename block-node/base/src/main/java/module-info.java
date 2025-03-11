// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.server.config.BlockNodeConfigExtension;

module org.hiero.block.base {
    exports org.hiero.block.server.consumer;
    exports org.hiero.block.server.exception;
    exports org.hiero.block.server.persistence.storage;
    exports org.hiero.block.server.persistence.storage.archive;
    exports org.hiero.block.server.persistence.storage.compression;
    exports org.hiero.block.server.persistence.storage.path;
    exports org.hiero.block.server.persistence.storage.write;
    exports org.hiero.block.server.persistence.storage.read;
    exports org.hiero.block.server.persistence.storage.remove;
    exports org.hiero.block.server.config;
    exports org.hiero.block.server.config.logging;
    exports org.hiero.block.server.mediator;
    exports org.hiero.block.server.metrics;
    exports org.hiero.block.server.events;
    exports org.hiero.block.server.health;
    exports org.hiero.block.server.ack;
    exports org.hiero.block.server.persistence;
    exports org.hiero.block.server.notifier;
    exports org.hiero.block.server.service;
    exports org.hiero.block.server.producer;
    exports org.hiero.block.server.verification;
    exports org.hiero.block.server.verification.session;
    exports org.hiero.block.server.verification.signature;
    exports org.hiero.block.server.verification.service;
    exports org.hiero.block.server.block;

    requires org.hiero.block.common;
    requires com.hedera.block.stream;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires com.swirlds.metrics.api;
    requires com.github.luben.zstd_jni;
    requires com.lmax.disruptor;
    requires dagger;
    requires io.helidon.webserver;
    requires javax.inject;
    requires static transitive com.github.spotbugs.annotations;
    requires static transitive com.google.auto.service;
    requires static java.compiler; // javax.annotation.processing.Generated

    provides com.swirlds.config.api.ConfigurationExtension with
            BlockNodeConfigExtension;
}
