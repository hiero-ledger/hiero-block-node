// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Per-client throttle settings for {@code getBlock} requests classified as historical/archived
 * (see {@link GetBlockWeigher}), plus the threshold that defines that classification. The
 * node-wide concurrency ceiling for this tier lives separately, in the shared
 * {@code GlobalThrottleConfig} (app-config) — see {@code docs/design/apis/api-throttling.md}
 * ("Configuration ownership") for why.
 *
 * @param ratePerSecond sustained requests per second allowed for one client
 * @param burstTolerance how many pacing intervals early a client's request may arrive and still
 *     be admitted
 * @param maxConcurrentPerClient maximum concurrent in-flight calls for one client
 * @param historicalThresholdBlocks a request for a block more than this many blocks behind the
 *     current maximum available block is classified as historical rather than live. This is a
 *     self-contained approximation of the recent-tier's actual retention boundary — deliberately
 *     independent of {@code files.recent.blockRetentionThreshold} rather than coupled to it, so
 *     this plugin doesn't depend on a specific storage-tier plugin being present. The default
 *     matches that setting's own default (96,000); an operator changing one should consider
 *     whether the other should change too, since nothing enforces them staying in sync. This is
 *     flagged as an assumption needing validation in {@code docs/design/apis/api-throttling.md} §9.
 */
@ConfigData("throttle.getBlockHistorical")
public record GetBlockHistoricalThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1)
        int ratePerSecond,

        @Loggable @ConfigProperty(defaultValue = "3") @Min(0)
        int burstTolerance,

        @Loggable @ConfigProperty(defaultValue = "3") @Min(1)
        int maxConcurrentPerClient,

        @Loggable @ConfigProperty(defaultValue = "96_000") @Min(1)
        long historicalThresholdBlocks) {}
