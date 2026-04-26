// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the RSA roster bootstrap plugin.
 *
 * @param filePath path to the local binary protobuf bootstrap file
 * @param mirrorNodeBaseUrl base URL of the Mirror Node REST API used when the file is absent
 * @param mirrorNodeConnectTimeoutSeconds TCP connect timeout when calling the Mirror Node
 * @param mirrorNodeReadTimeoutSeconds per-request read timeout when calling the Mirror Node
 * @param mirrorNodePageSize number of nodes requested per paginated Mirror Node call
 */
@ConfigData("roster.bootstrap")
public record BootstrapRosterConfig(
        @Loggable @ConfigProperty(defaultValue = "data/config/rsa-bootstrap-roster.pb")
                Path filePath,
        @Loggable @ConfigProperty(defaultValue = "https://testnet-public.mirrornode.hedera.com")
                String mirrorNodeBaseUrl,
        @Loggable @ConfigProperty(defaultValue = "5") int mirrorNodeConnectTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "10") int mirrorNodeReadTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "100") int mirrorNodePageSize) {}
