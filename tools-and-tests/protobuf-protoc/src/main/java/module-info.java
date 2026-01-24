// SPDX-License-Identifier: Apache-2.0
open module org.hiero.block.protobuf.protoc {
    exports com.hedera.hapi.services.auxiliary.hints.legacy;
    exports com.hedera.hapi.services.auxiliary.tss.legacy;
    exports com.hedera.hapi.services.auxiliary.history.legacy;
    exports com.hedera.hapi.node.hooks.legacy;
    exports com.hedera.hapi.node.state.hooks.legacy;
    exports com.hedera.hapi.node.state.tss.legacy;
    exports com.hedera.hapi.node.state.token.legacy;
    exports com.hedera.hapi.node.tss.legacy;
    exports com.hedera.services.stream.proto;
    exports com.hederahashgraph.api.proto.java;
    exports com.hedera.hapi.block.stream.protoc;
    exports com.hedera.hapi.block.stream.input.protoc;
    exports com.hedera.hapi.block.stream.output.protoc;
    exports com.hedera.hapi.block.stream.trace.protoc;
    exports com.hedera.hapi.platform.event.legacy;
    exports com.hedera.hapi.platform.state.legacy;
    exports org.hiero.block.api.protoc;
    exports org.hiero.block.internal.protoc;

    requires transitive com.google.common;
    requires transitive com.google.protobuf;
    requires transitive io.grpc.stub;
    requires transitive io.grpc;
    requires io.grpc.protobuf;
    requires static com.github.spotbugs.annotations;
    requires static java.annotation;
}
