// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/// Configuration for the RSA roster bootstrap plugin.
///
/// The RSA bootstrap file path is configured via `app.state.rsaBootstrapFilePath` in
/// `ApplicationStateConfig`; file loading and persistence are handled by `BlockNodeApp`.
///
/// @param mirrorNodeBaseUrl base URL of the Mirror Node REST API used when no local file is present;
///     leave blank to disable Mirror Node fallback (a WARNING is logged at startup if blank and no
///     bootstrap file is present)
/// @param mirrorNodeConnectTimeoutSeconds TCP connect timeout when calling the Mirror Node
/// @param mirrorNodeReadTimeoutSeconds per-request read timeout when calling the Mirror Node
/// @param mirrorNodePageSize number of nodes requested per paginated Mirror Node call
@ConfigData("roster.bootstrap.rsa")
public record RsaRosterBootstrapConfig(
        @Loggable @ConfigProperty(defaultValue = "") String mirrorNodeBaseUrl,
        @Loggable @ConfigProperty(defaultValue = "5") int mirrorNodeConnectTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "10") int mirrorNodeReadTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "100") int mirrorNodePageSize) {}
