// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.NavigableSet;
import java.util.TreeSet;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for the [ActiveSessionsSnapshot].
@DisplayName("Active Sessions Snapshot Tests")
class ActiveSessionsSnapshotTest {
    /// This test aims to assert that the snapshot copies the key sets it is
    /// constructed from, so that later changes to the live lanes are not visible
    /// through the snapshot.
    @Test
    @DisplayName("constructor copies both key sets")
    void testCopiesTheKeySetsItIsGiven() {
        final NavigableSet<SessionKey> highPriority = new TreeSet<>();
        highPriority.add(new SessionKey(1, 0));
        final NavigableSet<SessionKey> lowPriority = new TreeSet<>();
        lowPriority.add(new SessionKey(2, 1));
        final ActiveSessionsSnapshot snapshot = new ActiveSessionsSnapshot(highPriority, lowPriority, null, 0L);
        highPriority.add(new SessionKey(3, 2));
        lowPriority.add(new SessionKey(3, 3));
        assertThat(snapshot.highPrioritySessions()).containsExactly(new SessionKey(1, 0));
        assertThat(snapshot.lowPrioritySessions()).containsExactly(new SessionKey(2, 1));
        assertThat(snapshot.size()).isEqualTo(2);
    }

    /// This test aims to assert that the key sets exposed by the snapshot reject
    /// modification, so a policy cannot alter the view it was given.
    @Test
    @DisplayName("key sets cannot be modified")
    void testExposesUnmodifiableKeySets() {
        assertThatThrownBy(this::addToHighPrioritySessions).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(this::addToLowPrioritySessions).isInstanceOf(UnsupportedOperationException.class);
    }

    /// This test aims to assert that a snapshot may carry no active publisher
    /// session (null), which is the state between publisher blocks, while a null
    /// key set is rejected.
    @Test
    @DisplayName("active high priority session may be absent, key sets may not")
    void testAllowsAMissingActiveHighPrioritySessionAndRejectsNullKeySets() {
        final ActiveSessionsSnapshot snapshot = new ActiveSessionsSnapshot(new TreeSet<>(), new TreeSet<>(), null, -1L);
        assertThat(snapshot.activeHighPrioritySession()).isNull();
        assertThat(snapshot.lastVerifiedBlock()).isEqualTo(-1L);
        assertThatNullPointerException().isThrownBy(this::constructWithNullHighPrioritySessions);
        assertThatNullPointerException().isThrownBy(this::constructWithNullLowPrioritySessions);
    }

    /// Adds a key to the high priority sessions of a fresh snapshot.
    private void addToHighPrioritySessions() {
        new ActiveSessionsSnapshot(new TreeSet<>(), new TreeSet<>(), null, 0L)
                .highPrioritySessions()
                .add(new SessionKey(1, 0));
    }

    /// Adds a key to the low priority sessions of a fresh snapshot.
    private void addToLowPrioritySessions() {
        new ActiveSessionsSnapshot(new TreeSet<>(), new TreeSet<>(), null, 0L)
                .lowPrioritySessions()
                .add(new SessionKey(1, 0));
    }

    /// Constructs a snapshot with a null high priority key set.
    private void constructWithNullHighPrioritySessions() {
        new ActiveSessionsSnapshot(null, new TreeSet<>(), null, 0L);
    }

    /// Constructs a snapshot with a null low priority key set.
    private void constructWithNullLowPrioritySessions() {
        new ActiveSessionsSnapshot(new TreeSet<>(), null, null, 0L);
    }
}
