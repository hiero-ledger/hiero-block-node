// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.simulator.config.SimulatorConfigExtension;

module org.hiero.block.simulator {
    exports org.hiero.block.simulator.config.data;
    exports org.hiero.block.simulator.exception;
    exports org.hiero.block.simulator;
    exports org.hiero.block.simulator.config.types;
    exports org.hiero.block.simulator.config;
    exports org.hiero.block.simulator.grpc;
    exports org.hiero.block.simulator.generator;
    exports org.hiero.block.simulator.metrics;
    exports org.hiero.block.simulator.grpc.impl;
    exports org.hiero.block.simulator.mode;
    exports org.hiero.block.simulator.mode.impl;

    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.common;
    requires org.hiero.block.protobuf;
    requires com.google.protobuf;
    requires dagger;
    requires io.grpc.stub;
    requires io.grpc;
    requires java.logging;
    requires javax.inject;
    requires static transitive com.github.spotbugs.annotations;
    requires static transitive com.google.auto.service;
    requires static java.compiler; // javax.annotation.processing.Generated

    provides com.swirlds.config.api.ConfigurationExtension with
            SimulatorConfigExtension;
}
