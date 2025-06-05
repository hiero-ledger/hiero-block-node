// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.logging;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

@ConfigData("test")
public record TestSecretConfig(
        @ConfigProperty(defaultValue = "secretValue") String secret,
        @ConfigProperty(defaultValue = "") String emptySecret) {}
