// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import com.swirlds.state.binary.MerkleProof;
import com.swirlds.state.binary.QueueState;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.base.crypto.Hash;

/**
 * In-memory {@link BinaryState} fixture used by unit tests that exercise
 * {@link StateChangeApplier} without spinning up a real {@code VirtualMapStateImpl}.
 * Only the read / write surface the applier and the plugin queries actually touch
 * is implemented; the Merkle-proof methods throw because they are out of scope
 * for the live-state plugin v1.
 */
final class InMemoryBinaryState implements BinaryState {

    private final Map<Integer, Bytes> singletons = new HashMap<>();
    private final Map<Integer, Map<Bytes, Bytes>> kvs = new HashMap<>();
    private final Map<Integer, Deque<Bytes>> queues = new HashMap<>();

    @Nullable
    @Override
    public Bytes getKv(final int stateId, @NonNull final Bytes key) {
        final Map<Bytes, Bytes> map = kvs.get(stateId);
        return map == null ? null : map.get(key);
    }

    @Nullable
    @Override
    public Bytes getSingleton(final int singletonId) {
        return singletons.get(singletonId);
    }

    @Nullable
    @Override
    public QueueState getQueueState(final int stateId) {
        throw new UnsupportedOperationException("getQueueState not used by live-state plugin v1");
    }

    @Nullable
    @Override
    public Bytes peekQueueHead(final int stateId) {
        final Deque<Bytes> q = queues.get(stateId);
        return q == null ? null : q.peekFirst();
    }

    @Nullable
    @Override
    public Bytes peekQueueTail(final int stateId) {
        final Deque<Bytes> q = queues.get(stateId);
        return q == null ? null : q.peekLast();
    }

    @Nullable
    @Override
    public Bytes peekQueue(final int stateId, final int index) {
        final Deque<Bytes> q = queues.get(stateId);
        if (q == null || index < 0 || index >= q.size()) return null;
        int i = 0;
        for (final Bytes b : q) {
            if (i++ == index) return b;
        }
        return null;
    }

    @NonNull
    @Override
    public List<Bytes> getQueueAsList(final int stateId) {
        final Deque<Bytes> q = queues.get(stateId);
        return q == null ? List.of() : new ArrayList<>(q);
    }

    @Override
    public void updateSingleton(final int stateId, @NonNull final Bytes value) {
        singletons.put(stateId, value);
    }

    @Override
    public void removeSingleton(final int stateId) {
        singletons.remove(stateId);
    }

    @Override
    public void updateKv(final int stateId, @NonNull final Bytes key, @Nullable final Bytes value) {
        if (value == null) {
            removeKv(stateId, key);
            return;
        }
        kvs.computeIfAbsent(stateId, k -> new HashMap<>()).put(key, value);
    }

    @Override
    public void removeKv(final int stateId, @NonNull final Bytes key) {
        final Map<Bytes, Bytes> map = kvs.get(stateId);
        if (map != null) {
            map.remove(key);
            if (map.isEmpty()) kvs.remove(stateId);
        }
    }

    @Override
    public void pushQueue(final int stateId, @NonNull final Bytes value) {
        queues.computeIfAbsent(stateId, k -> new ArrayDeque<>()).addLast(value);
    }

    @Nullable
    @Override
    public Bytes popQueue(final int stateId) {
        final Deque<Bytes> q = queues.get(stateId);
        if (q == null) return null;
        final Bytes head = q.pollFirst();
        if (q.isEmpty()) queues.remove(stateId);
        return head;
    }

    @Override
    public void removeQueue(final int stateId) {
        queues.remove(stateId);
    }

    /** Test-only total entry count for assertions. */
    long size() {
        long total = singletons.size();
        for (final Map<Bytes, Bytes> m : kvs.values()) total += m.size();
        for (final Deque<Bytes> q : queues.values()) total += q.size();
        return total;
    }

    @Override
    public long getSingletonPath(final int stateId) {
        throw new UnsupportedOperationException("Merkle path resolution not used by live-state plugin v1");
    }

    @Override
    public long getQueueElementPath(final int stateId, @NonNull final Bytes expectedValue) {
        throw new UnsupportedOperationException("Merkle path resolution not used by live-state plugin v1");
    }

    @Override
    public long getKvPath(final int stateId, @NonNull final Bytes key) {
        throw new UnsupportedOperationException("Merkle path resolution not used by live-state plugin v1");
    }

    @Nullable
    @Override
    public Hash getHashForPath(final long path) {
        throw new UnsupportedOperationException("Merkle proof not used by live-state plugin v1");
    }

    @Nullable
    @Override
    public MerkleProof getMerkleProof(final long path) {
        throw new UnsupportedOperationException("Merkle proof not used by live-state plugin v1");
    }
}
