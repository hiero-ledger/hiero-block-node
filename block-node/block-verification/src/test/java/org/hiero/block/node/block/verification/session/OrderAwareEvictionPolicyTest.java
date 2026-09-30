// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [OrderAwareEvictionPolicy], exercised on synthetic snapshots
/// without any session, executor or verification.
@DisplayName("Order Aware Eviction Policy Tests")
class OrderAwareEvictionPolicyTest {
    /// The instance under test, built from the default configuration.
    private OrderAwareEvictionPolicy toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        toTest = new OrderAwareEvictionPolicy(config(Map.of()));
    }

    /// Tests for the first step of the policy, the low priority lane.
    @Nested
    @DisplayName("Low Priority Lane Selection")
    class LowPriorityLaneSelection {
        /// This test aims to assert that a snapshot without any session yields an
        /// empty selection, since there is nothing to give up.
        @Test
        @DisplayName("selectForEviction() returns nothing for empty lanes")
        void testSelectsNothingWhenBothLanesAreEmpty() {
            assertThat(toTest.selectForEviction(snapshot(keys(), keys(), null, 6L)))
                    .isEmpty();
        }

        /// This test aims to assert that when every session sits at the next expected
        /// block (nothing awaits order, so nothing is needed), the highest key of the
        /// low priority lane is selected, and that among two sessions for the same
        /// block the newer one (higher unique id) goes.
        @Test
        @DisplayName("selectForEviction() selects the highest low priority session when no session awaits order")
        void testSelectsTheHighestLowPrioritySessionWhenNothingAwaitsOrder() {
            final NavigableSet<SessionKey> lowPriority = keys(key(11, 0), key(11, 1));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 10L)))
                    .containsExactly(key(11, 1));
        }

        /// This test aims to assert that a low priority session whose block is at or
        /// below the last verified block is selected first, even though the highest
        /// low priority session is needed by a high priority session above it, so that a
        /// high priority session is never given up while a session that can never be
        /// useful exists.
        @Test
        @DisplayName("selectForEviction() selects a stale low priority session before anything else")
        void testSelectsTheLowestStaleLowPrioritySessionFirst() {
            final NavigableSet<SessionKey> lowPriority =
                    keys(key(45, 0), key(48, 1), key(55, 2), key(56, 3), key(57, 4));
            final NavigableSet<SessionKey> highPriority = keys(key(60, 5));
            assertThat(toTest.selectForEviction(snapshot(highPriority, lowPriority, null, 50L)))
                    .containsExactly(key(45, 0));
        }

        /// This test aims to assert that with a chain of awaiting low priority sessions
        /// and no high priority session, the highest one is selected: every lower session
        /// is needed by the sessions above it, while the highest is needed by nobody.
        @Test
        @DisplayName(
                "selectForEviction() selects the highest awaiting low priority session because nobody waits for it")
        void testSelectsTheHighestLowPrioritySessionThatNobodyWaitsFor() {
            final NavigableSet<SessionKey> lowPriority = keys(key(8, 0), key(9, 1), key(10, 2), key(16, 3));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 6L)))
                    .containsExactly(key(16, 3));
        }

        /// This test aims to assert that when a complete high priority session far ahead of
        /// the last verified block waits for the low priority sessions below it, none
        /// of those is selected and the high priority session is, mirroring a backfill
        /// catch-up under a parked publisher block.
        @Test
        @DisplayName(
                "selectForEviction() keeps needed low priority sessions and selects the far-ahead high priority session")
        void testKeepsLowPrioritySessionsThatAnAwaitingHighPrioritySessionNeeds() {
            final NavigableSet<SessionKey> highPriority = keys(key(10, 0));
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 1), key(8, 2), key(9, 3));
            assertThat(toTest.selectForEviction(snapshot(highPriority, lowPriority, null, 6L)))
                    .containsExactly(key(10, 0));
        }

        /// This test aims to assert that the high priority session still receiving its items
        /// is not counted as awaiting order, so the low priority sessions below it are
        /// not protected on its behalf and the highest of them is selected.
        @Test
        @DisplayName(
                "selectForEviction() ignores the high priority session still receiving items when computing what is needed")
        void testTheActiveHighPrioritySessionDoesNotProtectSessionsBelowIt() {
            final SessionKey active = key(20, 0);
            final NavigableSet<SessionKey> highPriority = keys(active);
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 1), key(8, 2), key(9, 3));
            assertThat(toTest.selectForEviction(snapshot(highPriority, lowPriority, active, 6L)))
                    .containsExactly(key(9, 3));
        }

        /// This test aims to assert that when blocks arrive in descending order into a
        /// full lane, each admission selects the highest session, so the lane keeps the
        /// blocks closest to release and progress resumes as soon as the missing low
        /// block arrives; this is the case in which the former lowest-block rule
        /// selected nothing.
        @Test
        @DisplayName("selectForEviction() selects the highest session on every admission of a descending delivery")
        void testEvictsTheHighestSessionOnEveryAdmissionOfADescendingDelivery() {
            final long lastVerified = 4L;
            final NavigableSet<SessionKey> first = keys(key(10, 0), key(9, 1), key(8, 2), key(7, 3));
            assertThat(toTest.selectForEviction(snapshot(keys(), first, null, lastVerified)))
                    .containsExactly(key(10, 0));
            final NavigableSet<SessionKey> second = keys(key(9, 1), key(8, 2), key(7, 3), key(6, 4));
            assertThat(toTest.selectForEviction(snapshot(keys(), second, null, lastVerified)))
                    .containsExactly(key(9, 1));
            final NavigableSet<SessionKey> third = keys(key(8, 2), key(7, 3), key(6, 4), key(5, 5));
            assertThat(toTest.selectForEviction(snapshot(keys(), third, null, lastVerified)))
                    .containsExactly(key(8, 2));
        }

        /// This test aims to assert that when the highest low priority block has two
        /// sessions, the one with the higher unique id (the newer) is selected, keeping
        /// the older session that is further along.
        @Test
        @DisplayName("selectForEviction() selects the newer of two sessions for the highest block")
        void testPrefersTheNewerOfTwoSessionsForTheSameBlock() {
            final NavigableSet<SessionKey> lowPriority = keys(key(55, 0), key(56, 1), key(57, 2), key(57, 5));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 50L)))
                    .containsExactly(key(57, 5));
        }

        /// This test aims to assert that with `allSourcesRequireOrdering` set to false the
        /// low priority sessions never await order, so a waiting high priority session alone
        /// decides what is needed: with a high priority session at 500 the backfilled
        /// sessions 7 to 16 are needed and the high priority session is selected; without
        /// any high priority session nothing is needed and the highest backfilled session is
        /// selected.
        @Test
        @DisplayName(
                "selectForEviction() treats low priority sessions as never awaiting order when allSourcesRequireOrdering is false")
        void testLowPrioritySessionsNeverAwaitOrderWhenNotAllSourcesRequireOrdering() {
            final OrderAwareEvictionPolicy policy =
                    new OrderAwareEvictionPolicy(config(Map.of("verification.allSourcesRequireOrdering", "false")));
            final NavigableSet<SessionKey> lowPriority = new TreeSet<>();
            for (long block = 7L; block <= 16L; block++) {
                lowPriority.add(key(block, block - 6L));
            }
            assertThat(policy.selectForEviction(snapshot(keys(key(500, 0)), lowPriority, null, 6L)))
                    .containsExactly(key(500, 0));
            assertThat(policy.selectForEviction(snapshot(keys(), lowPriority, null, 6L)))
                    .containsExactly(key(16, 10));
        }
    }

    /// Tests for the second step of the policy, the high priority lane.
    @Nested
    @DisplayName("High Priority Lane Selection")
    class HighPriorityLaneSelection {
        /// This test aims to assert that in a burst of high priority sessions the highest
        /// session other than the one still receiving items is selected, never the
        /// lowest, because the lowest is the next block to be released and the highest
        /// is the farthest from release.
        @Test
        @DisplayName("selectForEviction() selects the highest complete high priority session in a publisher only burst")
        void testSelectsTheHighestHighPrioritySessionOtherThanTheActiveOneInABurst() {
            final SessionKey active = key(11, 4);
            final NavigableSet<SessionKey> highPriority = keys(key(7, 0), key(8, 1), key(9, 2), key(10, 3), active);
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(), active, 6L)))
                    .containsExactly(key(10, 3));
        }

        /// This test aims to assert that the high priority session still receiving its items
        /// is never selected, even when it is the only session in the buffer, because
        /// its cancellation would report an incomplete block the publisher does not
        /// resend.
        @Test
        @DisplayName("selectForEviction() never selects the high priority session still receiving items")
        void testSelectsNothingWhenOnlyTheActiveHighPrioritySessionExists() {
            final SessionKey active = key(11, 0);
            assertThat(toTest.selectForEviction(snapshot(keys(active), keys(), active, 6L)))
                    .isEmpty();
        }

        /// This test aims to assert that when the low priority lane holds only sessions
        /// that the far-ahead high priority session is waiting for, the high priority session is
        /// selected.
        @Test
        @DisplayName("selectForEviction() selects a far-ahead high priority session over needed backfilled sessions")
        void testEvictsAFarAheadHighPrioritySessionBeforeNeededLowPrioritySessions() {
            final NavigableSet<SessionKey> highPriority = keys(key(500, 0));
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 1), key(8, 2), key(9, 3), key(10, 4));
            assertThat(toTest.selectForEviction(snapshot(highPriority, lowPriority, null, 6L)))
                    .containsExactly(key(500, 0));
        }

        /// This test aims to assert that the policy departs from the former lowest-block
        /// rule in the high priority lane: with high priority sessions 7, 8, 9 and nothing else,
        /// 9 is selected and 7, the next block to be released, is kept.
        @Test
        @DisplayName("selectForEviction() does not select the lowest high priority session")
        void testNeverSelectsTheLowestHighPrioritySessionWhileAHigherOneExists() {
            final NavigableSet<SessionKey> highPriority = keys(key(7, 0), key(8, 1), key(9, 2));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(), null, 6L)))
                    .containsExactly(key(9, 2));
        }

        /// This test aims to assert that after a resend the high priority session still
        /// receiving items can sit below complete sessions, and the highest complete
        /// session is still the one selected.
        @Test
        @DisplayName("selectForEviction() skips the active high priority session wherever it sits")
        void testSkipsTheActiveHighPrioritySessionEvenWhenItIsNotTheHighest() {
            final SessionKey active = key(8, 0);
            final NavigableSet<SessionKey> highPriority = keys(active, key(9, 1), key(10, 2));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(), active, 6L)))
                    .containsExactly(key(10, 2));
        }
    }

    /// Tests for the boundaries of the ordering rule the policy mirrors.
    @Nested
    @DisplayName("Ordering Boundary Cases")
    class OrderingBoundaryCases {
        /// This test aims to assert that before any block has been verified (last
        /// verified block -1) every session above block 0 awaits order, block 0 is the
        /// next expected block, and the highest complete high priority session is selected.
        @Test
        @DisplayName("selectForEviction() handles the startup state without a last verified block")
        void testTreatsEverySessionAboveZeroAsAwaitingOrderBeforeTheFirstBlockIsVerified() {
            final SessionKey active = key(4, 4);
            final NavigableSet<SessionKey> highPriority = keys(key(0, 0), key(1, 1), key(2, 2), key(3, 3), active);
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(), active, -1L)))
                    .containsExactly(key(3, 3));
        }

        /// This test aims to assert that sessions below `firstOrderedBlock` never await
        /// order, so with only such sessions nothing is needed and the highest is
        /// selected, while with an ordered high priority session above them they are kept
        /// and the high priority session is selected.
        @Test
        @DisplayName("selectForEviction() never treats sessions below the first ordered block as awaiting order")
        void testSessionsBelowTheFirstOrderedBlockNeverAwaitOrder() {
            final OrderAwareEvictionPolicy policy =
                    new OrderAwareEvictionPolicy(config(Map.of("verification.firstOrderedBlock", "100")));
            final NavigableSet<SessionKey> lowPriority = keys(key(95, 0), key(96, 1), key(97, 2));
            assertThat(policy.selectForEviction(snapshot(keys(), lowPriority, null, -1L)))
                    .containsExactly(key(97, 2));
            assertThat(policy.selectForEviction(snapshot(keys(key(100, 3)), lowPriority, null, -1L)))
                    .containsExactly(key(100, 3));
        }

        /// This test aims to assert that a session for the next expected block never
        /// awaits order itself, is needed when a session above it waits and is then
        /// kept, and is selected when nothing waits above it.
        @Test
        @DisplayName("selectForEviction() keeps the next expected block when a session above it waits")
        void testTheNextExpectedSessionNeverAwaitsOrderAndIsKeptWhenNeeded() {
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 0));
            assertThat(toTest.selectForEviction(snapshot(keys(key(9, 1)), lowPriority, null, 6L)))
                    .containsExactly(key(9, 1));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 6L)))
                    .containsExactly(key(7, 0));
        }
    }

    /// Tests for the validation of the policy's input.
    @Nested
    @DisplayName("Input Contract")
    class InputContract {
        /// This test aims to assert that the policy validates its input and rejects a
        /// null snapshot with a `NullPointerException`.
        @Test
        @DisplayName("selectForEviction() rejects a null snapshot")
        void testRejectsANullSnapshot() {
            assertThatNullPointerException().isThrownBy(this::selectWithNullSnapshot);
        }

        /// Calls the policy with a null snapshot.
        private void selectWithNullSnapshot() {
            toTest.selectForEviction(null);
        }
    }

    /// Build a verification configuration with the given overrides on top of the defaults.
    private static VerificationConfig config(final Map<String, String> overrides) {
        TestConfigurationBuilder builder = new TestConfigurationBuilder().withConfigDataType(VerificationConfig.class);
        for (final Map.Entry<String, String> override : overrides.entrySet()) {
            builder = builder.withValue(override.getKey(), override.getValue());
        }
        return builder.getOrCreateConfig().getConfigData(VerificationConfig.class);
    }

    /// A session key.
    private static SessionKey key(final long blockNumber, final long uniqueId) {
        return new SessionKey(blockNumber, uniqueId);
    }

    /// An ascending set of keys.
    private static NavigableSet<SessionKey> keys(final SessionKey... keys) {
        return new TreeSet<>(List.of(keys));
    }

    /// A snapshot.
    private static ActiveSessionsSnapshot snapshot(
            final NavigableSet<SessionKey> highPriority,
            final NavigableSet<SessionKey> lowPriority,
            final SessionKey activeHighPrioritySession,
            final long lastVerifiedBlock) {
        return new ActiveSessionsSnapshot(highPriority, lowPriority, activeHighPrioritySession, lastVerifiedBlock);
    }
}
