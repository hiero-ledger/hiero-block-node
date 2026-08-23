// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.throttle.ContentAwareWeigher;
import org.hiero.block.node.spi.throttle.WeightClass;

/// Classifies a `getBlock` request as [WeightClass#STANDARD] (live/recent) or
/// [WeightClass#HEAVY] (historical/archived), based on how far the requested block sits behind
/// the current maximum available block.
///
/// A request for the latest block, or one that cannot be classified (malformed, or missing a
/// block number and not marked `retrieveLatest`), is always treated as [WeightClass#STANDARD] —
/// it is either inherently cheap (latest) or will be rejected as an invalid request by
/// [BlockAccessServicePlugin] itself, not by the throttle.
///
/// This duplicates the parse that [BlockAccessServicePlugin]'s own `mapRequest` step performs on
/// the same bytes; see `agent/proposals/api-throttling/impl-findings-and-pivots.md` for why this
/// was accepted as a documented performance trade-off rather than solved with a zero-copy field
/// peek in this first pass.
final class GetBlockWeigher implements ContentAwareWeigher {
    private final HistoricalBlockFacility blockProvider;
    private final long historicalThresholdBlocks;

    GetBlockWeigher(@NonNull final HistoricalBlockFacility blockProvider, final long historicalThresholdBlocks) {
        this.blockProvider = blockProvider;
        this.historicalThresholdBlocks = historicalThresholdBlocks;
    }

    @NonNull
    @Override
    public WeightClass classify(@NonNull final Method method, @NonNull final Bytes requestBytes) {
        final BlockRequest request;
        try {
            request = standardParse(BlockRequest.PROTOBUF, requestBytes);
        } catch (final ParseException e) {
            return WeightClass.STANDARD;
        }

        if (!request.hasBlockNumber() || request.blockNumber() < 0) {
            // retrieveLatest, or malformed/absent — either way, not a historical lookup.
            return WeightClass.STANDARD;
        }

        final long maxAvailable = blockProvider.availableBlocks().max();
        final long distanceFromTip = maxAvailable - request.blockNumber();
        return distanceFromTip > historicalThresholdBlocks ? WeightClass.HEAVY : WeightClass.STANDARD;
    }
}
