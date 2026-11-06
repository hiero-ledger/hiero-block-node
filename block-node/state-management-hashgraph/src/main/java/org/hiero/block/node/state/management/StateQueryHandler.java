// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import com.swirlds.state.binary.QueueState;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import org.hiero.block.api.QueueRange;
import org.hiero.block.api.QueueValues;
import org.hiero.block.api.StateQuery;
import org.hiero.block.api.StateResponse;
import org.hiero.block.api.StateResponse.Code;
import org.hiero.block.node.state.management.StateManagementPlugin.AttestedSnapshot;
import org.hiero.metrics.LongCounter;

/// Answers `StateService` queries against the plugin's attested snapshot. Holds every query
/// rule (validation, block matching, reads, queue caps) so they can be tested against a real
/// `VirtualMapState` without the plugin lifecycle. See `docs/design/state/live-state.md` §8.
///
/// **Reader reservation contract.** Each request reads the published `AttestedSnapshot` once
/// and `tryReserve()`s its `VirtualMap` root before touching the state, releasing it when done.
/// If the reservation fails the apply thread has promoted a newer snapshot and released this
/// one, so the request retries with the newly published snapshot. A failed reservation on the
/// snapshot that is still published is a defect (`publishAttested` reserves the new root
/// before publishing and releases the old one only after) and throws `IllegalStateException`.
/// The reserved snapshot is used for every later step, so a response's block number and data
/// always come from the same state.
final class StateQueryHandler {

    private static final System.Logger LOGGER = System.getLogger(StateQueryHandler.class.getName());

    /// `block_number` on a `NOT_READY` response: `uint64` max on the wire, so it can't be
    /// confused with the valid genesis block `0`.
    static final long NOT_READY_BLOCK_NUMBER = -1L;

    /// Largest valid `state_id`: the protobuf field-number maximum (2^29 - 1). `VirtualMapState`
    /// keys tag the id as the int `stateId << 3`, so a larger id overflows and aliases another
    /// state's key.
    static final long MAX_STATE_ID = (1L << 29) - 1L;

    /// Smallest valid `state_id` for KV and queue reads. Field number 1 tags singleton keys, so a
    /// KV or queue key for state 1 would collide with a singleton key.
    static final long MIN_KV_OR_QUEUE_STATE_ID = 2L;

    /// Label name for the query kind on `state_query_total`.
    static final String LABEL_KIND = "kind";

    /// Label name for the response status on `state_query_total`.
    static final String LABEL_STATUS = "status";

    /// The query kinds, labelled by their `state_query_total` `kind` value.
    private enum Kind {
        KV("kv"),
        SINGLETON("singleton"),
        QUEUE("queue");

        private final String label;

        Kind(@NonNull final String label) {
            this.label = label;
        }
    }

    private final Supplier<AttestedSnapshot> attested;
    private final int maxQueueRangeSize;
    private final LongCounter queryCounter;

    /// Create a handler.
    ///
    /// @param attested supplies the currently published attested snapshot
    /// @param maxQueueRangeSize the most queue elements a single `getQueue` may return
    /// @param queryCounter the `state_query_total` counter, with `kind` and `status` labels
    StateQueryHandler(
            @NonNull final Supplier<AttestedSnapshot> attested,
            final int maxQueueRangeSize,
            @NonNull final LongCounter queryCounter) {
        this.attested = Objects.requireNonNull(attested);
        this.maxQueueRangeSize = maxQueueRangeSize;
        this.queryCounter = Objects.requireNonNull(queryCounter);
    }

    /// Read a key-value entry by `state_id` and `key_bytes`.
    ///
    /// @param query the query
    /// @return the response
    @NonNull
    StateResponse getKV(@NonNull final StateQuery query) {
        return handle(Kind.KV, query);
    }

    /// Read a singleton by `state_id`.
    ///
    /// @param query the query
    /// @return the response
    @NonNull
    StateResponse getSingleton(@NonNull final StateQuery query) {
        return handle(Kind.SINGLETON, query);
    }

    /// Read a whole queue, or a 1-indexed `queue_range` of it, by `state_id`.
    ///
    /// @param query the query
    /// @return the response
    @NonNull
    StateResponse getQueue(@NonNull final StateQuery query) {
        return handle(Kind.QUEUE, query);
    }

    /// Answer `query` and count the response by kind and status.
    @NonNull
    private StateResponse handle(@NonNull final Kind kind, @NonNull final StateQuery query) {
        final StateResponse response = reserveAndAnswer(kind, query);
        queryCounter
                .getOrCreateLabeled(
                        LABEL_KIND,
                        kind.label,
                        LABEL_STATUS,
                        response.status().name().toLowerCase(Locale.ROOT))
                .increment();
        return response;
    }

    /// Reserve the published snapshot's root (retrying if a newer one replaced it), answer
    /// against it, then release it. See the class javadoc for the contract.
    @NonNull
    private StateResponse reserveAndAnswer(@NonNull final Kind kind, @NonNull final StateQuery query) {
        AttestedSnapshot snap = attested.get();
        while (true) {
            final VirtualMapState state = snap.state();
            if (state == null) {
                return status(Code.NOT_READY, NOT_READY_BLOCK_NUMBER);
            }
            final VirtualMap root = state.getRoot();
            if (root.tryReserve()) {
                try {
                    return answer(kind, query, state, snap.blockNumber());
                } finally {
                    root.release();
                }
            }
            final AttestedSnapshot latest = attested.get();
            if (latest == snap) {
                throw new IllegalStateException(
                        "Published attested state for block " + snap.blockNumber() + " was already released");
            }
            snap = latest;
        }
    }

    /// Answer `query` against a reserved state attested at `blockNumber`.
    @NonNull
    private StateResponse answer(
            @NonNull final Kind kind,
            @NonNull final StateQuery query,
            @NonNull final BinaryState state,
            final long blockNumber) {
        if (!isValid(kind, query)) {
            return status(Code.INVALID_REQUEST, blockNumber);
        }
        if (query.hasBlockNumber() && query.blockNumberOrThrow() != blockNumber) {
            return status(Code.NOT_AVAILABLE, blockNumber);
        }
        final long stateId = query.stateId();
        if (!isAddressable(kind, stateId)) {
            return status(Code.NOT_FOUND, blockNumber);
        }
        try {
            return switch (kind) {
                case KV -> readKv(state, (int) stateId, query.keyBytesOrThrow(), blockNumber);
                case SINGLETON -> readSingleton(state, (int) stateId, blockNumber);
                case QUEUE -> readQueue(state, (int) stateId, query.queueRange(), blockNumber);
            };
        } catch (final RuntimeException e) {
            // VirtualMapStateImpl throws NPE/IAE on unknown state ids; treat as absent.
            LOGGER.log(
                    System.Logger.Level.DEBUG,
                    "State read failed for {0} query on state_id {1}; answering NOT_FOUND",
                    kind.label,
                    Long.toUnsignedString(stateId),
                    e);
            return status(Code.NOT_FOUND, blockNumber);
        }
    }

    /// Whether `stateId` can name a state of this kind without its key aliasing another state's.
    /// Singletons start at 1 (0 is `UNKNOWN`); KV and queue states start at 2. Unsigned, since
    /// `state_id` is `uint64` on the wire.
    private static boolean isAddressable(@NonNull final Kind kind, final long stateId) {
        final long min = kind == Kind.SINGLETON ? 1L : MIN_KV_OR_QUEUE_STATE_ID;
        return Long.compareUnsigned(stateId, min) >= 0 && Long.compareUnsigned(stateId, MAX_STATE_ID) <= 0;
    }

    /// Validate the block specifier and the selector shape for `kind`.
    private boolean isValid(@NonNull final Kind kind, @NonNull final StateQuery query) {
        final boolean blockSpecified =
                query.hasBlockNumber() || (query.hasRetrieveLatest() && query.retrieveLatestOrThrow());
        if (!blockSpecified) {
            return false;
        }
        return switch (kind) {
            // An empty key_bytes is valid: an all-default key message encodes to empty.
            case KV -> query.hasKeyBytes();
            case SINGLETON -> !query.hasKeyBytes() && !query.hasQueueRange();
            case QUEUE -> !query.hasKeyBytes() && isValidRange(query.queueRange());
        };
    }

    /// A range is valid when absent, `0..0` (full read), or `1 <= start <= end` spanning at most
    /// `maxQueueRangeSize` elements. All comparisons are unsigned (`uint64` on the wire).
    private boolean isValidRange(@Nullable final QueueRange range) {
        if (range == null || isFullRead(range)) {
            return true;
        }
        final long start = range.start();
        final long end = range.end();
        if (start == 0L || end == 0L || Long.compareUnsigned(start, end) > 0) {
            return false;
        }
        return Long.compareUnsigned(end - start, maxQueueRangeSize - 1L) <= 0;
    }

    /// `true` when `range` is `0..0`, which selects the whole queue like an absent range.
    private static boolean isFullRead(@NonNull final QueueRange range) {
        return range.start() == 0L && range.end() == 0L;
    }

    @NonNull
    private static StateResponse readKv(
            @NonNull final BinaryState state, final int stateId, @NonNull final Bytes key, final long blockNumber) {
        final Bytes value = state.getKv(stateId, key);
        if (value == null) {
            return status(Code.NOT_FOUND, blockNumber);
        }
        return StateResponse.newBuilder()
                .status(Code.SUCCESS)
                .blockNumber(blockNumber)
                .kvBytes(value)
                .build();
    }

    @NonNull
    private static StateResponse readSingleton(
            @NonNull final BinaryState state, final int stateId, final long blockNumber) {
        final Bytes value = state.getSingleton(stateId);
        if (value == null) {
            return status(Code.NOT_FOUND, blockNumber);
        }
        return StateResponse.newBuilder()
                .status(Code.SUCCESS)
                .blockNumber(blockNumber)
                .singletonBytes(value)
                .build();
    }

    /// Read the whole queue or a validated 1-indexed range. Both read element by element via
    /// `peekQueue`, so a missing element fails the whole request the same way for either shape
    /// (`getQueueAsList` would silently skip it). A range is never partially served.
    @NonNull
    private StateResponse readQueue(
            @NonNull final BinaryState state,
            final int stateId,
            @Nullable final QueueRange range,
            final long blockNumber) {
        final QueueState queueState = state.getQueueState(stateId);
        if (queueState == null) {
            return status(Code.NOT_FOUND, blockNumber);
        }
        final long size = queueState.tail() - queueState.head();
        final long first;
        final long last;
        if (range == null || isFullRead(range)) {
            if (size > maxQueueRangeSize) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Full read of queue state_id {0} rejected: {1} elements exceeds"
                                + " maxQueueRangeSize {2}; client should page with queue_range",
                        stateId,
                        size,
                        maxQueueRangeSize);
                return status(Code.INVALID_REQUEST, blockNumber);
            }
            first = 1L;
            last = size;
        } else {
            if (Long.compareUnsigned(range.end(), size) > 0) {
                return status(Code.NOT_FOUND, blockNumber);
            }
            first = range.start();
            last = range.end();
        }
        final List<Bytes> values = new ArrayList<>((int) Math.max(0L, last - first + 1L));
        for (long k = first; k <= last; k++) {
            // peekQueue's index is absolute in [head, tail); queue_range is 1-indexed.
            final Bytes value = state.peekQueue(stateId, Math.toIntExact(queueState.head() + k - 1L));
            if (value == null) {
                return status(Code.NOT_FOUND, blockNumber);
            }
            values.add(value);
        }
        return StateResponse.newBuilder()
                .status(Code.SUCCESS)
                .blockNumber(blockNumber)
                .queueValues(new QueueValues(values))
                .build();
    }

    /// A response with `status` and `blockNumber` and no payload.
    @NonNull
    private static StateResponse status(@NonNull final Code status, final long blockNumber) {
        return StateResponse.newBuilder()
                .status(status)
                .blockNumber(blockNumber)
                .build();
    }
}
