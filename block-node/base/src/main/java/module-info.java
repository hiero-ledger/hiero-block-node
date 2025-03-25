// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.base {
    exports org.hiero.block.node.base;

    requires com.github.luben.zstd_jni;
    requires static transitive com.github.spotbugs.annotations;
    requires static java.compiler; // javax.annotation.processing.Generated
}
