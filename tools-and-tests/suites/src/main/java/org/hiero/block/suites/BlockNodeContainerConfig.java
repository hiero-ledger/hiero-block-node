// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

/**
 * Configuration for a single Block Node instance in test suites.
 */
public record BlockNodeContainerConfig(int port, int metricPort, String backfillSourcePath) {}
