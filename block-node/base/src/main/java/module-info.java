// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.base {
    exports org.hiero.block.node.base.config;
    exports org.hiero.block.node.base;

    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.config.extensions;
    requires transitive dagger;
    requires transitive javax.inject;
    requires com.swirlds.common;
    requires com.github.luben.zstd_jni;
    requires static transitive com.github.spotbugs.annotations;
    requires static transitive com.google.auto.service;
    requires static java.compiler; // javax.annotation.processing.Generated
}
