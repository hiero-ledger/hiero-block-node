// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config.node;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration for Node-wide configuration.
 * <p>
 * Node-wide configuration includes settings useful to _all_ or nearly all
 * plugins. Examples include the earliest block number managed by this node.
 *
 * @param earliestManagedBlock the block number for the earliest block managed
 *     by this node. Blocks earlier than this might be present, but the node
 *     should not make any particular effort to obtain or store them.
 */
@ConfigData("block.node")
public record NodeConfig(@Loggable @ConfigProperty(defaultValue = "0") @Min(0) long earliestManagedBlock) {}
