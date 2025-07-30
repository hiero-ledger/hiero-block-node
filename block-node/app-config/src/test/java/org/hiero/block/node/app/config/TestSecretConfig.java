// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

@SuppressWarnings("unused")
@ConfigData("test")
public record TestSecretConfig(
        @ConfigProperty(defaultValue = "secretValue") String secret,
        @ConfigProperty(defaultValue = "") String emptySecret) {}
