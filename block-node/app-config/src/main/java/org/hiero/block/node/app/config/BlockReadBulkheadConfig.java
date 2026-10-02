// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the shared block-storage read bulkhead; see {@code BlockReadBulkhead} for
 * what it protects and why it's shared.
 *
 * @param permits the fixed size of the bulkhead's permit pool
 */
@ConfigData("blockReadBulkhead")
public record BlockReadBulkheadConfig(
        @Loggable @ConfigProperty(defaultValue = "50") @Min(1)
        int permits) {}
