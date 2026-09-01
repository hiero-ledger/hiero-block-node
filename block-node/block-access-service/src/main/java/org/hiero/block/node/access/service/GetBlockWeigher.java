// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.BufferUnderflowException;
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
/// Reads only the `block_number` field (field 1) directly off the wire, without fully
/// deserializing the request — see `docs/design/apis/api-throttling.md` ("Performance") for why: a
/// weigher must not pay for a second full parse of a message [BlockAccessServicePlugin]'s own
/// `mapRequest` step will parse in full anyway once the call is admitted.
final class GetBlockWeigher implements ContentAwareWeigher {
    private static final int BLOCK_NUMBER_FIELD = 1;
    /// `block_number` is part of a `oneof` with `retrieve_latest`, so PBJ always writes it to the
    /// wire when it's the active branch, even if its value is `0` — unlike a plain scalar field,
    /// there's no ambiguity between "absent" and "present with the default value" to worry about
    /// here. This sentinel means the field was not found on the wire at all (the oneof is either
    /// `retrieve_latest` or unset).
    private static final long BLOCK_NUMBER_NOT_PRESENT = Long.MIN_VALUE;

    private final HistoricalBlockFacility blockProvider;
    private final long historicalThresholdBlocks;

    GetBlockWeigher(@NonNull final HistoricalBlockFacility blockProvider, final long historicalThresholdBlocks) {
        this.blockProvider = blockProvider;
        this.historicalThresholdBlocks = historicalThresholdBlocks;
    }

    @NonNull
    @Override
    public WeightClass classify(@NonNull final Method method, @NonNull final Bytes requestBytes) {
        final long blockNumber;
        try {
            blockNumber = readBlockNumber(requestBytes);
        } catch (final IOException e) {
            return WeightClass.STANDARD;
        }

        if (blockNumber == BLOCK_NUMBER_NOT_PRESENT || blockNumber < 0) {
            // retrieveLatest, unset, or malformed — either way, not a historical lookup.
            return WeightClass.STANDARD;
        }

        final long maxAvailable = blockProvider.availableBlocks().max();
        final long distanceFromTip = maxAvailable - blockNumber;
        return distanceFromTip > historicalThresholdBlocks ? WeightClass.HEAVY : WeightClass.STANDARD;
    }

    /// Reads the `block_number` field directly from the wire, skipping every other field without
    /// decoding it. Returns [#BLOCK_NUMBER_NOT_PRESENT] if the field is absent.
    private static long readBlockNumber(@NonNull final Bytes requestBytes) throws IOException {
        final ReadableSequentialData input = requestBytes.toReadableSequentialData();
        while (input.hasRemaining()) {
            final int tag;
            try {
                tag = input.readVarInt(false);
            } catch (final BufferUnderflowException e) {
                break;
            }
            final int field = tag >>> ProtoParserTools.TAG_FIELD_OFFSET;
            final ProtoConstants wireType = ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK);
            if (field == BLOCK_NUMBER_FIELD) {
                return ProtoParserTools.readUint64(input);
            }
            ProtoParserTools.skipField(input, wireType);
        }
        return BLOCK_NUMBER_NOT_PRESENT;
    }
}
