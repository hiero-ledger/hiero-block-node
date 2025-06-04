// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.protobuf.protoc {
    exports com.hedera.hapi.platform.state.legacy to
            com.google.protobuf;
    exports org.hiero.block.api.protoc to
            org.hiero.block.simulator,
            org.hiero.block.node.suites;
    exports org.hiero.block.internal.protoc to
            org.hiero.block.simulator;
    exports com.hedera.services.stream.proto to
            org.hiero.block.simulator;
    exports com.hederahashgraph.api.proto.java to
            org.hiero.block.simulator;
    exports com.hederahashgraph.service.proto.java to
            org.hiero.block.simulator;
    exports com.hedera.hapi.block.stream.protoc to
            org.hiero.block.simulator,
            org.hiero.block.node.suites;
    exports com.hedera.hapi.block.stream.input.protoc to
            org.hiero.block.simulator,
            org.hiero.block.node.suites;
    exports com.hedera.hapi.block.stream.output.protoc to
            org.hiero.block.simulator,
            org.hiero.block.node.suites;
    exports com.hedera.hapi.platform.event.legacy to
            com.google.protobuf,
            org.hiero.block.simulator;

    requires transitive com.google.common;
    requires transitive com.google.protobuf;
    requires transitive io.grpc.stub;
    requires transitive io.grpc;
    requires io.grpc.protobuf;
    requires static com.github.spotbugs.annotations;
    requires static java.annotation;

    // only open protoc to com.google.protobuf
    opens org.hiero.block.api.protoc to
            com.google.protobuf;
    opens org.hiero.block.internal.protoc to
            com.google.protobuf;
    opens com.hedera.services.stream.proto to
            com.google.protobuf;
    opens com.hedera.hapi.block.stream.input.protoc to
            com.google.protobuf;
    opens com.hedera.hapi.block.stream.output.protoc to
            com.google.protobuf;
    opens com.hedera.hapi.block.stream.protoc to
            com.google.protobuf;
    opens com.hedera.hapi.platform.state.legacy to
            com.google.protobuf;
    opens com.hedera.hapi.node.state.tss.legacy to
            com.google.protobuf;
    opens com.hedera.hapi.services.auxiliary.hints.legacy to
            com.google.protobuf;
    opens com.hedera.hapi.services.auxiliary.history.legacy to
            com.google.protobuf;
    opens com.hedera.hapi.services.auxiliary.tss.legacy to
            com.google.protobuf;
    opens com.hederahashgraph.api.proto.java to
            com.google.protobuf;
    opens com.hederahashgraph.service.proto.java to
            com.google.protobuf;
}
