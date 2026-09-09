// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the shared block-storage read bulkhead: a single, bounded, non-client-keyed
 * permit pool protecting block storage from combined read load across every API that reads from
 * it, shared across every call path that draws on it via {@code ServiceBuilder.blockReadBulkhead()}.
 *
 * @param permits the fixed size of the bulkhead's permit pool
 */
@ConfigData("blockReadBulkhead")
public record BlockReadBulkheadConfig(
        @Loggable @ConfigProperty(defaultValue = "50") @Min(1)
        int permits) {}
