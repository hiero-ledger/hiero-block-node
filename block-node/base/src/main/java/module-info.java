// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.base {
    exports org.hiero.block.node.base;
    exports org.hiero.block.node.base.client;
    exports org.hiero.block.node.base.ranges;
    exports org.hiero.block.node.base.s3;
    exports org.hiero.block.node.base.tar;

    requires transitive com.hedera.pbj.runtime;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.pbj.grpc.client.helidon;
    requires org.hiero.block.common;
    requires com.github.luben.zstd_jni;
    requires io.helidon.common.tls;
    requires io.helidon.webclient.api;
    requires io.helidon.webclient.grpc;
    requires io.helidon.webclient.http2;
    requires java.net.http;
    requires java.xml;
    requires static transitive com.github.spotbugs.annotations;
    requires static java.compiler; // javax.annotation.processing.Generated
}
