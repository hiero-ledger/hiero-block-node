// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

// spotless:off - long annotations on record components must stay on one line
/// Configuration for the live Hashgraph state plugin. Defaults match the production
/// filesystem layout described in `docs/design/state/live-state.md`.
@ConfigData("state.management")
public record StateManagementConfig(
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/recent")
        String snapshotRecentPath) {
    // spotless:on
}
