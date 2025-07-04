// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.common {
    exports org.hiero.block.common.constants;
    exports org.hiero.block.common.utils;
    exports org.hiero.block.common.hasher;

    requires transitive com.hedera.pbj.runtime;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.swirlds.common;
    requires static com.github.spotbugs.annotations;
}
