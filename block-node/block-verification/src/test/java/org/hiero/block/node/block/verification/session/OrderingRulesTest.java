// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for the [OrderingRules], the rules the ordering stage and the eviction policy
/// share. Unless a test says otherwise, the rules are applied with the default
/// configuration: every source requires ordering, and ordering starts at block 0.
@DisplayName("Ordering Rules Tests")
class OrderingRulesTest {
    /// The default configuration for verification.
    private static final VerificationConfig DEFAULTS = config(Map.of());

    /// Tests for which sources and lanes are subject to ordering.
    @Nested
    @DisplayName("Subject To Ordering")
    class SubjectToOrdering {
        /// This test aims to assert that publisher blocks, and so the sessions of the
        /// high priority lane, are always subject to ordering, whether all sources
        /// require ordering or not.
        @ParameterizedTest(name = "allSourcesRequireOrdering {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("requiresOrdering() is true for the publisher and the high priority lane")
        void testThePublisherIsAlwaysSubjectToOrdering(final boolean allSourcesRequireOrdering) {
            final VerificationConfig config = allSourcesRequireOrdering(allSourcesRequireOrdering);
            assertThat(OrderingRules.requiresOrdering(config, BlockSource.PUBLISHER))
                    .isTrue();
            assertThat(OrderingRules.requiresOrdering(config, SessionLane.HIGH_PRIORITY))
                    .isTrue();
        }

        /// This test aims to assert that the blocks of every source other than the
        /// publisher, and so the sessions of the low priority lane, are subject to
        /// ordering only when all sources require ordering. Every such source is
        /// exercised.
        @ParameterizedTest(name = "{0}")
        @EnumSource(
                value = BlockSource.class,
                names = {"PUBLISHER"},
                mode = EnumSource.Mode.EXCLUDE)
        @DisplayName("requiresOrdering() follows allSourcesRequireOrdering for every other source and the low lane")
        void testEveryOtherSourceFollowsAllSourcesRequireOrdering(final BlockSource source) {
            assertThat(OrderingRules.requiresOrdering(allSourcesRequireOrdering(true), source))
                    .isTrue();
            assertThat(OrderingRules.requiresOrdering(allSourcesRequireOrdering(false), source))
                    .isFalse();
            assertThat(OrderingRules.requiresOrdering(allSourcesRequireOrdering(true), SessionLane.LOW_PRIORITY))
                    .isTrue();
            assertThat(OrderingRules.requiresOrdering(allSourcesRequireOrdering(false), SessionLane.LOW_PRIORITY))
                    .isFalse();
        }
    }

    /// Tests for whether a block waits for its turn.
    @Nested
    @DisplayName("Waits For Order")
    class WaitsForOrder {
        /// This test aims to assert that a block of a source subject to ordering waits
        /// only when it is ahead of the next expected block, and a block of any other
        /// source never waits. Every source is exercised under both settings of
        /// `allSourcesRequireOrdering`: the publisher is always subject to ordering,
        /// every other source only when all sources require it. With the last verified
        /// block 100, the blocks 50, 100 and 101, the next expected one, are released at
        /// once, while 102 and 500 wait exactly when the source is subject to ordering.
        @ParameterizedTest(name = "{0}, allSourcesRequireOrdering {1}")
        @CsvSource({
            "PUBLISHER, true, true",
            "PUBLISHER, false, true",
            "BACKFILL, true, true",
            "BACKFILL, false, false",
            "HISTORY, true, true",
            "HISTORY, false, false",
            "UNKNOWN, true, true",
            "UNKNOWN, false, false"
        })
        @DisplayName(
                "waitsForOrder() for a source is true only ahead of the next expected block, if the source is ordered")
        void testASourceWaitsOnlyAheadOfTheNextExpectedBlock(
                final BlockSource source, final boolean allSourcesRequireOrdering, final boolean subjectToOrdering) {
            final VerificationConfig config = allSourcesRequireOrdering(allSourcesRequireOrdering);
            assertThat(OrderingRules.waitsForOrder(config, 50, source, 100)).isFalse();
            assertThat(OrderingRules.waitsForOrder(config, 100, source, 100)).isFalse();
            assertThat(OrderingRules.waitsForOrder(config, 101, source, 100)).isFalse();
            assertThat(OrderingRules.waitsForOrder(config, 102, source, 100)).isEqualTo(subjectToOrdering);
            assertThat(OrderingRules.waitsForOrder(config, 500, source, 100)).isEqualTo(subjectToOrdering);
        }

        /// This test aims to assert that a session of a lane subject to ordering waits
        /// only when its block is ahead of the next expected block, and a session of the
        /// other lane never waits. Both lanes are exercised under both settings of
        /// `allSourcesRequireOrdering`: the high priority lane, which holds the
        /// publisher's blocks, is always subject to ordering, the low priority lane only
        /// when all sources require it. With the last verified block 100, the blocks 50,
        /// 100 and 101 are released at once, while 102 and 500 wait exactly when the lane
        /// is subject to ordering.
        @ParameterizedTest(name = "{0}, allSourcesRequireOrdering {1}")
        @CsvSource({
            "HIGH_PRIORITY, true, true",
            "HIGH_PRIORITY, false, true",
            "LOW_PRIORITY, true, true",
            "LOW_PRIORITY, false, false"
        })
        @DisplayName("waitsForOrder() for a lane is true only ahead of the next expected block, if the lane is ordered")
        void testALaneWaitsOnlyAheadOfTheNextExpectedBlock(
                final SessionLane lane, final boolean allSourcesRequireOrdering, final boolean subjectToOrdering) {
            final VerificationConfig config = allSourcesRequireOrdering(allSourcesRequireOrdering);
            assertThat(OrderingRules.waitsForOrder(config, 50, lane, 100)).isFalse();
            assertThat(OrderingRules.waitsForOrder(config, 100, lane, 100)).isFalse();
            assertThat(OrderingRules.waitsForOrder(config, 101, lane, 100)).isFalse();
            assertThat(OrderingRules.waitsForOrder(config, 102, lane, 100)).isEqualTo(subjectToOrdering);
            assertThat(OrderingRules.waitsForOrder(config, 500, lane, 100)).isEqualTo(subjectToOrdering);
        }

        /// This test aims to assert that before any block is verified (last verified
        /// block -1) no block waits, whatever its number, since the ordering stage
        /// adopts the first block that verifies as the last verified one: the blocks 0,
        /// 1 and 500 are all released at once.
        @Test
        @DisplayName("waitsForOrder() is false for every block before any block is verified")
        void testNothingWaitsBeforeAnyBlockIsVerified() {
            assertThat(publisherBlockWaits(DEFAULTS, 0, -1)).isFalse();
            assertThat(publisherBlockWaits(DEFAULTS, 1, -1)).isFalse();
            assertThat(publisherBlockWaits(DEFAULTS, 500, -1)).isFalse();
        }

        /// This test aims to assert that blocks below the first ordered block never wait:
        /// with the first ordered block 1000 and the last verified block 100, the block
        /// 999 is released at once although it is far ahead, while 1000 waits.
        @Test
        @DisplayName("waitsForOrder() is false below the first ordered block")
        void testDoesNotWaitBelowTheFirstOrderedBlock() {
            final VerificationConfig config = config(Map.of("verification.firstOrderedBlock", "1000"));
            assertThat(publisherBlockWaits(config, 999, 100)).isFalse();
            assertThat(publisherBlockWaits(config, 1000, 100)).isTrue();
        }

        /// This test aims to assert that a lane gets the same answer as the source it
        /// holds, so that the eviction policy, which sees lanes, and the ordering stage,
        /// which sees sources, never disagree: for every block from 0 to 120, the last
        /// verified blocks -1, 0 and 100, both settings of `allSourcesRequireOrdering` and
        /// the first ordered block 50, the high priority lane agrees with the publisher and
        /// the low priority lane with backfill.
        @Test
        @DisplayName("waitsForOrder() gives a lane the same answer as the source it holds")
        void testALaneAgreesWithTheSourceItHolds() {
            for (final boolean allSources : new boolean[] {true, false}) {
                final VerificationConfig config = config(Map.of(
                        "verification.allSourcesRequireOrdering",
                        String.valueOf(allSources),
                        "verification.firstOrderedBlock",
                        "50"));
                for (final long lastVerified : new long[] {-1L, 0L, 100L}) {
                    for (long block = 0; block <= 120; block++) {
                        assertThat(OrderingRules.waitsForOrder(config, block, SessionLane.HIGH_PRIORITY, lastVerified))
                                .isEqualTo(OrderingRules.waitsForOrder(
                                        config, block, BlockSource.PUBLISHER, lastVerified));
                        assertThat(OrderingRules.waitsForOrder(config, block, SessionLane.LOW_PRIORITY, lastVerified))
                                .isEqualTo(
                                        OrderingRules.waitsForOrder(config, block, BlockSource.BACKFILL, lastVerified));
                    }
                }
            }
        }
    }

    /// Whether a publisher block waits for its turn.
    private static boolean publisherBlockWaits(
            final VerificationConfig config, final long blockNumber, final long lastVerifiedBlock) {
        return OrderingRules.waitsForOrder(config, blockNumber, BlockSource.PUBLISHER, lastVerifiedBlock);
    }

    /// The default configuration with the given `allSourcesRequireOrdering` setting.
    private static VerificationConfig allSourcesRequireOrdering(final boolean allSourcesRequireOrdering) {
        return config(Map.of("verification.allSourcesRequireOrdering", String.valueOf(allSourcesRequireOrdering)));
    }

    /// Build a verification configuration with the given overrides on top of the defaults.
    private static VerificationConfig config(final Map<String, String> overrides) {
        TestConfigurationBuilder builder = new TestConfigurationBuilder().withConfigDataType(VerificationConfig.class);
        for (final Map.Entry<String, String> override : overrides.entrySet()) {
            builder = builder.withValue(override.getKey(), override.getValue());
        }
        return builder.getOrCreateConfig().getConfigData(VerificationConfig.class);
    }
}
