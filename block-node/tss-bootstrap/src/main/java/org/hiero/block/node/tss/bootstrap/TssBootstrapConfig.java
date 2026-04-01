// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

// spotless:off

import static com.swirlds.config.api.ConfigProperty.UNDEFINED_DEFAULT_VALUE;

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
        @Loggable @ConfigProperty(defaultValue = "") String ledgerId,
        @Loggable @ConfigProperty(defaultValue = "") String wrapsVerificationKey,
        @Loggable @ConfigProperty(defaultValue = "0") long nodeId,
        @Loggable @ConfigProperty(defaultValue = "0") long weight,
        @Loggable @ConfigProperty(defaultValue = "") String schnorrPublicKey) {}

// spotless:on
