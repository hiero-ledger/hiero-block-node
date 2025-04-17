// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.base {
    exports org.hiero.block.node.base;
    exports org.hiero.block.node.base.ranges;
    exports org.hiero.block.node.base.s3;

    requires transitive org.hiero.block.node.spi;
    requires com.github.luben.zstd_jni;
    requires java.net.http;
    requires java.xml;
    requires static transitive com.github.spotbugs.annotations;
    requires static java.compiler; // javax.annotation.processing.Generated
}
