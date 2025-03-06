// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.config;

import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.server.metrics.MetricsService;

/**
 * Context for the block node. This record is returned by the BlockNodeContextFactory when a new
 * configuration is created.
 *
 * @param metricsService the service responsible for handling metrics
 * @param configuration the configuration settings for the block node
 */
public record BlockNodeContext(@NonNull MetricsService metricsService, @NonNull Configuration configuration) {}
