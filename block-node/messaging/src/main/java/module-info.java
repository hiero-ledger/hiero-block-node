// SPDX-License-Identifier: Apache-2.0

module org.hiero.block.messaging {
    exports org.hiero.block.server.messaging;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.common;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.config.extensions;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.block.common;
    requires transitive org.hiero.block.stream;
    requires transitive com.lmax.disruptor;
    requires transitive dagger;
    requires transitive io.helidon.webserver;
    requires transitive javax.inject;
    requires static transitive com.github.spotbugs.annotations;
    requires static transitive com.google.auto.service;
    requires static java.compiler; // javax.annotation.processing.Generated
}
