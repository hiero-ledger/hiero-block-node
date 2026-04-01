// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

// spotless:off

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/// Configuration for the tss-bootstrap module.
///
/// @param tssParametersFilePath path where TSS parameters (ledger ID, address book, WRAPS VK)
///     are persisted across restarts as a serialized {@code TssData}.
@ConfigData("tss.bootstrap")
public record TssBootstrapConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/tss/bootstrap/tss-parameters.bin") Path tssParametersFilePath,
        @Loggable @ConfigProperty(defaultValue = "ZmU0MTA2ZWUwNGMzMzgyNTljZDcyMWEwN2Y0ZWFhZDMxMjEyZjEyZWFjOGE1MWFjYjM4YTc3OGE0Y2QyMDg3Mw==") String ledgerId,
        @Loggable @ConfigProperty(defaultValue = "NTUyZTAxNDNjMGE4MTBmNmYwN2VkOGUyZWI0ZGM1MTkwNWEzMmFkMzljZDQ5Yzk0MWE0MGU1YmM4YWEyZDg4NA==") String wrapsVerificationKey) {
}

// spotless:on
