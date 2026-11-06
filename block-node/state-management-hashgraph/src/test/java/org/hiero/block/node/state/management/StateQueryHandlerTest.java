// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.api.QueueRange;
import org.hiero.block.api.QueueValues;
import org.hiero.block.api.StateQuery;
import org.hiero.block.api.StateResponse;
import org.hiero.block.api.StateResponse.Code;
import org.hiero.block.node.app.fixtures.logging.TestLogHandler;
import org.hiero.block.node.state.management.StateManagementPlugin.AttestedSnapshot;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Exercises every `StateQueryHandler` rule against a real sealed `VirtualMapState`, without the
/// plugin lifecycle.
class StateQueryHandlerTest {

    private static final int CAP = 4;
    private static final long BLOCK = 7L;

    private static final int SINGLETON_ID = 5;
    private static final int KV_ID = 6;
    private static final int QUEUE_ID = 10;
    private static final int EMPTY_QUEUE_ID = 11;
    private static final int CAP_QUEUE_ID = 12;
    private static final int OVER_CAP_QUEUE_ID = 13;
    private static final int MAX_ID = (int) StateQueryHandler.MAX_STATE_ID;

    private static final Bytes SINGLETON_VALUE = Bytes.fromHex("c0ffee");
    private static final Bytes KV_KEY = Bytes.fromHex("0102");
    private static final Bytes KV_VALUE = Bytes.fromHex("aa01");
    private static final Bytes EMPTY_KEY_VALUE = Bytes.fromHex("aa02");
    private static final Bytes MAX_ID_VALUE = Bytes.fromHex("aa03");
    private static final Bytes ID_ONE_VALUE = Bytes.fromHex("aa04");
    private static final Bytes Q1 = Bytes.fromHex("01");
    private static final Bytes Q2 = Bytes.fromHex("02");
    private static final Bytes Q3 = Bytes.fromHex("03");

    @TempDir
    Path tmp;

    private VirtualMapState sealed;
    private VirtualMapState destroyed;
    private LongCounter counter;
    private final AtomicReference<AttestedSnapshot> attested = new AtomicReference<>();
    private StateQueryHandler handler;

    @BeforeEach
    void setUp() {
        final var config = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .build();
        final var manager = new VirtualMapStateLifecycleManager(
                new NoOpSwirldsMetrics(), Time.getCurrent(), config, new FileSystemManager(tmp));
        manager.copyMutableState();
        // The genesis copy is released by the manager on the next copy; nothing else reserves it.
        destroyed = manager.getLatestImmutableState();

        final VirtualMapState mutable = manager.getMutableState();
        mutable.updateSingleton(SINGLETON_ID, SINGLETON_VALUE);
        mutable.updateKv(KV_ID, KV_KEY, KV_VALUE);
        mutable.updateKv(KV_ID, Bytes.EMPTY, EMPTY_KEY_VALUE);
        mutable.updateKv(MAX_ID, KV_KEY, MAX_ID_VALUE);
        // Field 1 tags singleton keys, so state 1 is never addressable as KV even if present.
        mutable.updateKv(1, KV_KEY, ID_ONE_VALUE);
        // Pop the first element so head is 2: proves queue_range maps to absolute peek indexes.
        mutable.pushQueue(QUEUE_ID, Bytes.fromHex("00"));
        mutable.pushQueue(QUEUE_ID, Q1);
        mutable.pushQueue(QUEUE_ID, Q2);
        mutable.pushQueue(QUEUE_ID, Q3);
        mutable.popQueue(QUEUE_ID);
        mutable.pushQueue(EMPTY_QUEUE_ID, Q1);
        mutable.popQueue(EMPTY_QUEUE_ID);
        for (int i = 0; i < CAP; i++) {
            mutable.pushQueue(CAP_QUEUE_ID, Bytes.wrap(new byte[] {(byte) i}));
        }
        for (int i = 0; i <= CAP; i++) {
            mutable.pushQueue(OVER_CAP_QUEUE_ID, Bytes.wrap(new byte[] {(byte) i}));
        }
        manager.copyMutableState();
        sealed = manager.getLatestImmutableState();
        sealed.getRoot().getHash();
        // Reserve as publishAttested does.
        sealed.getRoot().reserve();
        attested.set(new AttestedSnapshot(sealed, BLOCK, 70L));

        counter = newQueryCounter();
        handler = new StateQueryHandler(attested::get, CAP, counter);
    }

    @AfterEach
    void tearDown() {
        sealed.getRoot().release();
    }

    @Test
    void notReadyForEveryKindCarriesUint64MaxBlockNumber() throws Exception {
        attested.set(new AttestedSnapshot(null, -1L, -1L));

        for (final StateResponse response : List.of(
                handler.getKV(latest().stateId(KV_ID).keyBytes(KV_KEY).build()),
                handler.getSingleton(latest().stateId(SINGLETON_ID).build()),
                handler.getQueue(latest().stateId(QUEUE_ID).build()))) {
            assertThat(response.status()).isEqualTo(Code.NOT_READY);
            assertThat(response.blockNumber()).isEqualTo(StateQueryHandler.NOT_READY_BLOCK_NUMBER);
            assertNoPayload(response);
            final StateResponse parsed = StateResponse.PROTOBUF.parse(StateResponse.PROTOBUF.toBytes(response));
            assertThat(parsed.blockNumber()).isEqualTo(-1L);
            assertThat(Long.toUnsignedString(parsed.blockNumber())).isEqualTo("18446744073709551615");
        }
        assertThat(count("kv", "not_ready") + count("singleton", "not_ready") + count("queue", "not_ready"))
                .isEqualTo(3L);
    }

    @Test
    void malformedRequestsAreInvalidAndCarryTheAttestedBlockNumber() {
        final List<StateResponse> responses = new ArrayList<>();
        // No block specifier, or retrieve_latest=false, for every kind.
        for (final Function<StateQuery.Builder, StateQuery.Builder> specifier :
                List.<Function<StateQuery.Builder, StateQuery.Builder>>of(b -> b, b -> b.retrieveLatest(false))) {
            responses.add(handler.getKV(specifier
                    .apply(StateQuery.newBuilder())
                    .stateId(KV_ID)
                    .keyBytes(KV_KEY)
                    .build()));
            responses.add(handler.getSingleton(specifier
                    .apply(StateQuery.newBuilder())
                    .stateId(SINGLETON_ID)
                    .build()));
            responses.add(handler.getQueue(
                    specifier.apply(StateQuery.newBuilder()).stateId(QUEUE_ID).build()));
        }
        // getKV needs key_bytes.
        responses.add(handler.getKV(latest().stateId(KV_ID).build()));
        responses.add(
                handler.getKV(latest().stateId(KV_ID).queueRange(range(1, 1)).build()));
        // getSingleton takes no selector, not even a 0..0 range. Validation precedes block matching.
        responses.add(handler.getSingleton(
                latest().stateId(SINGLETON_ID).keyBytes(KV_KEY).build()));
        responses.add(handler.getSingleton(
                latest().stateId(SINGLETON_ID).queueRange(range(0, 0)).build()));
        responses.add(handler.getSingleton(StateQuery.newBuilder()
                .blockNumber(BLOCK + 1)
                .stateId(SINGLETON_ID)
                .keyBytes(KV_KEY)
                .build()));
        // getQueue takes no key_bytes, and only 0..0 or 1 <= start <= end spanning <= CAP.
        responses.add(
                handler.getQueue(latest().stateId(QUEUE_ID).keyBytes(KV_KEY).build()));
        for (final QueueRange bad :
                List.of(range(0, 3), range(3, 0), range(3, 2), range(1, CAP + 1), range(2, CAP + 2), range(1, -1L))) {
            responses.add(
                    handler.getQueue(latest().stateId(QUEUE_ID).queueRange(bad).build()));
        }

        assertThat(responses).allSatisfy(response -> {
            assertThat(response.status()).isEqualTo(Code.INVALID_REQUEST);
            assertThat(response.blockNumber()).isEqualTo(BLOCK);
            assertNoPayload(response);
        });
    }

    @Test
    void olderBlockIsNotAvailable() {
        final long older = BLOCK - 1;
        assertNotAvailable(older);
        // NOT_AVAILABLE is decided before the state_id range and the full-queue cap.
        assertThat(handler.getKV(at(older).stateId(0L).keyBytes(KV_KEY).build()).status())
                .isEqualTo(Code.NOT_AVAILABLE);
        assertThat(handler.getQueue(at(older).stateId(OVER_CAP_QUEUE_ID).build())
                        .status())
                .isEqualTo(Code.NOT_AVAILABLE);
    }

    @Test
    void newerBlockIsNotAvailable() {
        assertNotAvailable(BLOCK + 1);
    }

    @Test
    void blockZeroIsServedWhenBlockZeroIsAttested() {
        attested.set(new AttestedSnapshot(sealed, 0L, 0L));

        final StateResponse response =
                handler.getSingleton(at(0L).stateId(SINGLETON_ID).build());

        assertThat(response.status()).isEqualTo(Code.SUCCESS);
        assertThat(response.blockNumber()).isZero();
        assertThat(response.singletonBytes()).isEqualTo(SINGLETON_VALUE);
    }

    @Test
    void kvAndSingletonReadsSucceed() {
        for (final Supplier<StateQuery.Builder> block :
                List.<Supplier<StateQuery.Builder>>of(StateQueryHandlerTest::latest, () -> at(BLOCK))) {
            final StateResponse kv =
                    handler.getKV(block.get().stateId(KV_ID).keyBytes(KV_KEY).build());
            assertThat(kv.status()).isEqualTo(Code.SUCCESS);
            assertThat(kv.blockNumber()).isEqualTo(BLOCK);
            assertThat(kv.kvBytes()).isEqualTo(KV_VALUE);

            final StateResponse singleton =
                    handler.getSingleton(block.get().stateId(SINGLETON_ID).build());
            assertThat(singleton.status()).isEqualTo(Code.SUCCESS);
            assertThat(singleton.blockNumber()).isEqualTo(BLOCK);
            assertThat(singleton.singletonBytes()).isEqualTo(SINGLETON_VALUE);
        }
        // An all-default key message encodes to empty key_bytes, which is a valid key.
        assertThat(handler.getKV(latest().stateId(KV_ID).keyBytes(Bytes.EMPTY).build())
                        .kvBytes())
                .isEqualTo(EMPTY_KEY_VALUE);
        // The top of the state_id range is addressable.
        assertThat(handler.getKV(latest().stateId(MAX_ID).keyBytes(KV_KEY).build())
                        .kvBytes())
                .isEqualTo(MAX_ID_VALUE);
    }

    @Test
    void queueReadsSucceedInHeadToTailOrder() {
        assertThat(queueValues(latest().stateId(QUEUE_ID).build())).containsExactly(Q1, Q2, Q3);
        assertThat(queueValues(
                        latest().stateId(QUEUE_ID).queueRange(range(0, 0)).build()))
                .containsExactly(Q1, Q2, Q3);
        assertThat(queueValues(
                        latest().stateId(QUEUE_ID).queueRange(range(2, 2)).build()))
                .containsExactly(Q2);
        assertThat(queueValues(
                        latest().stateId(QUEUE_ID).queueRange(range(1, 3)).build()))
                .containsExactly(Q1, Q2, Q3);
        assertThat(queueValues(
                        latest().stateId(QUEUE_ID).queueRange(range(2, 3)).build()))
                .containsExactly(Q2, Q3);

        final StateResponse empty =
                handler.getQueue(latest().stateId(EMPTY_QUEUE_ID).build());
        assertThat(empty.status()).isEqualTo(Code.SUCCESS);
        assertThat(empty.hasQueueValues()).isTrue();
        assertThat(empty.queueValuesOrThrow().values()).isEmpty();
    }

    @Test
    void absentEntriesAreNotFound() {
        final List<StateResponse> responses = List.of(
                handler.getKV(
                        latest().stateId(KV_ID).keyBytes(Bytes.fromHex("ff")).build()),
                handler.getKV(latest().stateId(999).keyBytes(KV_KEY).build()),
                handler.getSingleton(latest().stateId(999).build()),
                handler.getQueue(latest().stateId(999).build()),
                handler.getQueue(latest().stateId(999).queueRange(range(1, 1)).build()),
                // Range end past the size is never partially served.
                handler.getQueue(
                        latest().stateId(QUEUE_ID).queueRange(range(2, 4)).build()),
                handler.getQueue(
                        latest().stateId(EMPTY_QUEUE_ID).queueRange(range(1, 1)).build()),
                handler.getQueue(
                        latest().stateId(QUEUE_ID).queueRange(range(-1L, -1L)).build()));

        assertThat(responses).allSatisfy(response -> {
            assertThat(response.status()).isEqualTo(Code.NOT_FOUND);
            assertThat(response.blockNumber()).isEqualTo(BLOCK);
            assertNoPayload(response);
        });
    }

    @Test
    void stateIdsOutsideTheAddressableRangeAreNotFoundRatherThanAliased() {
        final long aliasOffset = StateQueryHandler.MAX_STATE_ID + 1L;
        // Why the bound exists: the impl tags keys with the int `stateId << 3`, which wraps here.
        assertThat(aliasOffset).isEqualTo(1L << 29);
        assertThat(((int) (aliasOffset + KV_ID)) << 3).isEqualTo(KV_ID << 3);
        // An unguarded read would succeed with another state's data.
        assertThat(sealed.getKv((int) (aliasOffset + KV_ID), KV_KEY)).isEqualTo(KV_VALUE);
        assertThat(sealed.getKv(1, KV_KEY)).isEqualTo(ID_ONE_VALUE);

        final List<StateResponse> responses = new ArrayList<>();
        for (final long id : List.of(aliasOffset + KV_ID, 0L, 1L, Integer.MAX_VALUE + 1L, -1L)) {
            responses.add(handler.getKV(latest().stateId(id).keyBytes(KV_KEY).build()));
        }
        for (final long id : List.of(aliasOffset + QUEUE_ID, 0L, 1L, Integer.MAX_VALUE + 1L, -1L)) {
            responses.add(handler.getQueue(latest().stateId(id).build()));
            responses.add(handler.getQueue(
                    latest().stateId(id).queueRange(range(1, 1)).build()));
        }
        for (final long id : List.of(aliasOffset + SINGLETON_ID, 0L, Integer.MAX_VALUE + 1L, -1L)) {
            responses.add(handler.getSingleton(latest().stateId(id).build()));
        }

        assertThat(responses).allSatisfy(response -> {
            assertThat(response.status()).isEqualTo(Code.NOT_FOUND);
            assertThat(response.blockNumber()).isEqualTo(BLOCK);
            assertNoPayload(response);
        });
    }

    @Test
    void queueCapAppliesToFullReadsAndRanges() {
        final TestLogHandler logHandler = new TestLogHandler();
        final Logger julLogger = Logger.getLogger(StateQueryHandler.class.getName());
        julLogger.addHandler(logHandler);
        final StateResponse overCapFull;
        try {
            overCapFull = handler.getQueue(latest().stateId(OVER_CAP_QUEUE_ID).build());
        } finally {
            julLogger.removeHandler(logHandler);
        }
        assertThat(overCapFull.status()).isEqualTo(Code.INVALID_REQUEST);
        assertThat(overCapFull.blockNumber()).isEqualTo(BLOCK);
        // The capture handler sees the raw message template, not the formatted parameters.
        assertThat(logHandler.countContaining("client should page with queue_range"))
                .isEqualTo(1);

        assertThat(queueValues(latest().stateId(CAP_QUEUE_ID).build())).hasSize(CAP);
        assertThat(queueValues(latest().stateId(OVER_CAP_QUEUE_ID)
                        .queueRange(range(1, CAP))
                        .build()))
                .hasSize(CAP);
        assertThat(queueValues(latest().stateId(OVER_CAP_QUEUE_ID)
                        .queueRange(range(2, CAP + 1))
                        .build()))
                .hasSize(CAP);
        assertThat(handler.getQueue(latest().stateId(OVER_CAP_QUEUE_ID)
                                .queueRange(range(1, CAP + 1))
                                .build())
                        .status())
                .isEqualTo(Code.INVALID_REQUEST);
    }

    @Test
    void queriesLeaveTheRootReservationCountUnchanged() {
        final int before = sealed.getRoot().getReservationCount();

        handler.getKV(latest().stateId(KV_ID).keyBytes(KV_KEY).build());
        handler.getKV(latest().stateId(999).keyBytes(KV_KEY).build());
        handler.getKV(latest().stateId(KV_ID).build());
        handler.getSingleton(at(BLOCK + 1).stateId(SINGLETON_ID).build());
        handler.getSingleton(latest().stateId(-1L).build());
        handler.getQueue(latest().stateId(QUEUE_ID).build());
        handler.getQueue(latest().stateId(OVER_CAP_QUEUE_ID).build());
        handler.getQueue(latest().stateId(QUEUE_ID).queueRange(range(2, 4)).build());

        assertThat(sealed.getRoot().getReservationCount()).isEqualTo(before);
    }

    @Test
    void releasedSnapshotIsRetriedAgainstTheNewlyPublishedOne() {
        assertThat(destroyed.getRoot().isDestroyed()).isTrue();
        final AttestedSnapshot stale = new AttestedSnapshot(destroyed, BLOCK - 1, 60L);
        final AttestedSnapshot live = attested.get();
        final AtomicInteger calls = new AtomicInteger();
        final StateQueryHandler retrying =
                new StateQueryHandler(() -> calls.getAndIncrement() == 0 ? stale : live, CAP, newQueryCounter());

        final StateResponse response =
                retrying.getSingleton(latest().stateId(SINGLETON_ID).build());

        assertThat(response.status()).isEqualTo(Code.SUCCESS);
        assertThat(response.blockNumber()).isEqualTo(BLOCK);
        assertThat(response.singletonBytes()).isEqualTo(SINGLETON_VALUE);
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void releasedSnapshotThatIsStillPublishedIsADefect() {
        assertThat(destroyed.getRoot().isDestroyed()).isTrue();
        final AttestedSnapshot stale = new AttestedSnapshot(destroyed, BLOCK, 70L);
        final StateQueryHandler stuck = new StateQueryHandler(() -> stale, CAP, newQueryCounter());

        assertThatThrownBy(
                        () -> stuck.getSingleton(latest().stateId(SINGLETON_ID).build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already released");
    }

    @Test
    void queryCounterCountsEachResponseByKindAndStatus() {
        handler.getKV(latest().stateId(KV_ID).keyBytes(KV_KEY).build());
        handler.getKV(latest().stateId(KV_ID).keyBytes(KV_KEY).build());
        handler.getKV(latest().stateId(KV_ID).keyBytes(Bytes.fromHex("ff")).build());
        handler.getSingleton(latest().stateId(SINGLETON_ID).keyBytes(KV_KEY).build());
        handler.getQueue(at(BLOCK - 1).stateId(QUEUE_ID).build());
        handler.getQueue(latest().stateId(QUEUE_ID).build());

        assertThat(count("kv", "success")).isEqualTo(2L);
        assertThat(count("kv", "not_found")).isEqualTo(1L);
        assertThat(count("singleton", "invalid_request")).isEqualTo(1L);
        assertThat(count("singleton", "success")).isZero();
        assertThat(count("queue", "not_available")).isEqualTo(1L);
        assertThat(count("queue", "success")).isEqualTo(1L);
    }

    @Test
    void oneofPresenceThatValidationReliesOnSurvivesTheWire() throws Exception {
        attested.set(new AttestedSnapshot(sealed, 0L, 0L));

        final StateQuery blockZero = roundTrip(at(0L).stateId(SINGLETON_ID).build());
        assertThat(blockZero.hasBlockNumber()).isTrue();
        assertThat(handler.getSingleton(blockZero).status()).isEqualTo(Code.SUCCESS);

        final StateQuery notLatest = roundTrip(StateQuery.newBuilder()
                .retrieveLatest(false)
                .stateId(SINGLETON_ID)
                .build());
        assertThat(notLatest.hasRetrieveLatest()).isTrue();
        assertThat(notLatest.retrieveLatestOrThrow()).isFalse();

        final StateQuery emptyKey =
                roundTrip(latest().stateId(KV_ID).keyBytes(Bytes.EMPTY).build());
        assertThat(emptyKey.hasKeyBytes()).isTrue();
        assertThat(handler.getKV(emptyKey).kvBytes()).isEqualTo(EMPTY_KEY_VALUE);

        final StateResponse emptyQueue =
                handler.getQueue(latest().stateId(EMPTY_QUEUE_ID).build());
        final StateResponse parsed = StateResponse.PROTOBUF.parse(StateResponse.PROTOBUF.toBytes(emptyQueue));
        assertThat(parsed.status()).isEqualTo(Code.SUCCESS);
        assertThat(parsed.blockNumber()).isZero();
        assertThat(parsed.hasQueueValues()).isTrue();
        assertThat(parsed.queueValuesOrThrow()).isEqualTo(new QueueValues(List.of()));
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private void assertNotAvailable(final long blockNumber) {
        assertThat(List.of(
                        handler.getKV(
                                at(blockNumber).stateId(KV_ID).keyBytes(KV_KEY).build()),
                        handler.getSingleton(
                                at(blockNumber).stateId(SINGLETON_ID).build()),
                        handler.getQueue(at(blockNumber).stateId(QUEUE_ID).build())))
                .allSatisfy(response -> {
                    assertThat(response.status()).isEqualTo(Code.NOT_AVAILABLE);
                    assertThat(response.blockNumber()).isEqualTo(BLOCK);
                    assertNoPayload(response);
                });
    }

    private List<Bytes> queueValues(final StateQuery query) {
        final StateResponse response = handler.getQueue(query);
        assertThat(response.status()).isEqualTo(Code.SUCCESS);
        assertThat(response.blockNumber()).isEqualTo(BLOCK);
        return response.queueValuesOrThrow().values();
    }

    private long count(final String kind, final String status) {
        return counter.getOrCreateLabeled(StateQueryHandler.LABEL_KIND, kind, StateQueryHandler.LABEL_STATUS, status)
                .get();
    }

    /// Built the way `StateManagementPlugin.registerMetrics` builds it.
    private static LongCounter newQueryCounter() {
        return MetricRegistry.builder()
                .build()
                .register(LongCounter.builder(StateManagementPlugin.METRIC_QUERY_TOTAL)
                        .addDynamicLabelNames(StateQueryHandler.LABEL_KIND, StateQueryHandler.LABEL_STATUS));
    }

    private static void assertNoPayload(final StateResponse response) {
        assertThat(response.hasKvBytes()).isFalse();
        assertThat(response.hasSingletonBytes()).isFalse();
        assertThat(response.hasQueueValues()).isFalse();
    }

    private static StateQuery roundTrip(final StateQuery query) throws Exception {
        return StateQuery.PROTOBUF.parse(StateQuery.PROTOBUF.toBytes(query));
    }

    private static StateQuery.Builder latest() {
        return StateQuery.newBuilder().retrieveLatest(true);
    }

    private static StateQuery.Builder at(final long blockNumber) {
        return StateQuery.newBuilder().blockNumber(blockNumber);
    }

    private static QueueRange range(final long start, final long end) {
        return QueueRange.newBuilder().start(start).end(end).build();
    }
}
