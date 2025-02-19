// SPDX-License-Identifier: Apache-2.0
import com.hedera.block.simulator.config.SimulatorConfigExtension;

module com.hedera.block.simulator {
    exports com.hedera.block.simulator.config.data;
    exports com.hedera.block.simulator.exception;
    exports com.hedera.block.simulator;
    exports com.hedera.block.simulator.config.types;
    exports com.hedera.block.simulator.config;
    exports com.hedera.block.simulator.grpc;
    exports com.hedera.block.simulator.generator;
    exports com.hedera.block.simulator.metrics;
    exports com.hedera.block.simulator.grpc.impl;
    exports com.hedera.block.simulator.mode;
    exports com.hedera.block.simulator.mode.impl;

    requires com.hedera.block.common;
    requires com.hedera.block.stream;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires com.swirlds.metrics.api;
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
