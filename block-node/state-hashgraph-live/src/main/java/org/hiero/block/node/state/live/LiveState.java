// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory live Hashgraph state used by {@link LiveStatePlugin}. Mirrors the
 * shape of {@code BinaryState} from {@code swirlds-state-api} — singletons,
 * key-value entries, and queues addressed by integer state-id — without taking
 * on {@code swirlds-state-impl} / {@code swirlds-virtualmap} as dependencies.
 *
 * <p>All values are raw protobuf {@link Bytes}; the caller is expected to encode
 * domain objects before writing and decode after reading. Mutation is guarded by
 * a {@link ReentrantReadWriteLock} so the apply loop and query path can run
 * concurrently without tearing.
 *
 * <p>{@link #computeHash()} produces a deterministic SHA-384 digest over the
 * current contents. The encoding is intentionally simple — see method
 * documentation — and is the input both to {@link LiveStateSnapshotIO} and to
 * the {@code state_root_hash} field of
 * {@link org.hiero.block.api.StateMetadata}.
 */
public final class LiveState {

    /** Algorithm used for every state root hash. */
    static final String HASH_ALG = "SHA-384";

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<Integer, Bytes> singletons = new HashMap<>();
    private final Map<Integer, Map<Bytes, Bytes>> kvs = new HashMap<>();
    private final Map<Integer, Deque<Bytes>> queues = new HashMap<>();

    // ── Reads ───────────────────────────────────────────────────────────────

    @Nullable
    public Bytes getSingleton(final int stateId) {
        lock.readLock().lock();
        try {
            return singletons.get(stateId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Nullable
    public Bytes getKv(final int stateId, @NonNull final Bytes key) {
        lock.readLock().lock();
        try {
            final Map<Bytes, Bytes> map = kvs.get(stateId);
            return map == null ? null : map.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @NonNull
    public List<Bytes> getQueueAsList(final int stateId) {
        lock.readLock().lock();
        try {
            final Deque<Bytes> q = queues.get(stateId);
            return q == null ? List.of() : new ArrayList<>(q);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Nullable
    public Bytes peekQueue(final int stateId, final int index) {
        lock.readLock().lock();
        try {
            final Deque<Bytes> q = queues.get(stateId);
            if (q == null || index < 0 || index >= q.size()) {
                return null;
            }
            int i = 0;
            for (final Bytes b : q) {
                if (i++ == index) {
                    return b;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ── Writes ──────────────────────────────────────────────────────────────

    public void updateSingleton(final int stateId, @NonNull final Bytes value) {
        lock.writeLock().lock();
        try {
            singletons.put(stateId, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeSingleton(final int stateId) {
        lock.writeLock().lock();
        try {
            singletons.remove(stateId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateKv(final int stateId, @NonNull final Bytes key, @NonNull final Bytes value) {
        lock.writeLock().lock();
        try {
            kvs.computeIfAbsent(stateId, k -> new HashMap<>()).put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeKv(final int stateId, @NonNull final Bytes key) {
        lock.writeLock().lock();
        try {
            final Map<Bytes, Bytes> map = kvs.get(stateId);
            if (map != null) {
                map.remove(key);
                if (map.isEmpty()) {
                    kvs.remove(stateId);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void pushQueue(final int stateId, @NonNull final Bytes value) {
        lock.writeLock().lock();
        try {
            queues.computeIfAbsent(stateId, k -> new ArrayDeque<>()).addLast(value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Nullable
    public Bytes popQueue(final int stateId) {
        lock.writeLock().lock();
        try {
            final Deque<Bytes> q = queues.get(stateId);
            if (q == null) {
                return null;
            }
            final Bytes head = q.pollFirst();
            if (q.isEmpty()) {
                queues.remove(stateId);
            }
            return head;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── Hashing & size ──────────────────────────────────────────────────────

    /**
     * Deterministic SHA-384 over a canonical encoding of the current state:
     *
     * <pre>
     *   singletons (sorted by state_id):  stateId | len(value) | value
     *   kvs        (sorted by state_id; entries sorted by key bytes):
     *                                     stateId | len(key)   | key | len(value) | value
     *   queues     (sorted by state_id; elements in insertion order):
     *                                     stateId | count      | (len(el) | el)*
     * </pre>
     *
     * Each section is preceded by a 4-byte tag (1 = singleton, 2 = kv, 3 = queue) and a
     * 4-byte count so the encoding is unambiguous.
     */
    @NonNull
    public Bytes computeHash() {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(HASH_ALG);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(HASH_ALG + " not available", e);
        }

        lock.readLock().lock();
        try {
            // Singletons
            md.update(intToBytes(1));
            md.update(intToBytes(singletons.size()));
            final TreeMap<Integer, Bytes> sortedSingletons = new TreeMap<>(singletons);
            for (final Map.Entry<Integer, Bytes> e : sortedSingletons.entrySet()) {
                md.update(intToBytes(e.getKey()));
                final byte[] v = e.getValue().toByteArray();
                md.update(intToBytes(v.length));
                md.update(v);
            }

            // KVs
            md.update(intToBytes(2));
            md.update(intToBytes(kvs.size()));
            final TreeMap<Integer, Map<Bytes, Bytes>> sortedKvs = new TreeMap<>(kvs);
            for (final Map.Entry<Integer, Map<Bytes, Bytes>> e : sortedKvs.entrySet()) {
                md.update(intToBytes(e.getKey()));
                md.update(intToBytes(e.getValue().size()));
                final List<Map.Entry<Bytes, Bytes>> entries = new ArrayList<>(e.getValue().entrySet());
                entries.sort(Comparator.comparing(Map.Entry::getKey, Bytes::compareTo));
                for (final Map.Entry<Bytes, Bytes> kv : entries) {
                    final byte[] k = kv.getKey().toByteArray();
                    final byte[] v = kv.getValue().toByteArray();
                    md.update(intToBytes(k.length));
                    md.update(k);
                    md.update(intToBytes(v.length));
                    md.update(v);
                }
            }

            // Queues
            md.update(intToBytes(3));
            md.update(intToBytes(queues.size()));
            final TreeMap<Integer, Deque<Bytes>> sortedQueues = new TreeMap<>(queues);
            for (final Map.Entry<Integer, Deque<Bytes>> e : sortedQueues.entrySet()) {
                md.update(intToBytes(e.getKey()));
                md.update(intToBytes(e.getValue().size()));
                for (final Bytes el : e.getValue()) {
                    final byte[] b = el.toByteArray();
                    md.update(intToBytes(b.length));
                    md.update(b);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return Bytes.wrap(md.digest());
    }

    /** Number of state entries currently held (singletons + KV pairs + queue elements). */
    public long size() {
        lock.readLock().lock();
        try {
            long total = singletons.size();
            for (final Map<Bytes, Bytes> m : kvs.values()) {
                total += m.size();
            }
            for (final Deque<Bytes> q : queues.values()) {
                total += q.size();
            }
            return total;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ── Package-visible helpers for snapshot I/O ────────────────────────────

    @NonNull
    Map<Integer, Bytes> singletonsSnapshot() {
        lock.readLock().lock();
        try {
            return new TreeMap<>(singletons);
        } finally {
            lock.readLock().unlock();
        }
    }

    @NonNull
    Map<Integer, Map<Bytes, Bytes>> kvsSnapshot() {
        lock.readLock().lock();
        try {
            final TreeMap<Integer, Map<Bytes, Bytes>> out = new TreeMap<>();
            for (final Map.Entry<Integer, Map<Bytes, Bytes>> e : kvs.entrySet()) {
                out.put(e.getKey(), new HashMap<>(e.getValue()));
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    @NonNull
    Map<Integer, List<Bytes>> queuesSnapshot() {
        lock.readLock().lock();
        try {
            final TreeMap<Integer, List<Bytes>> out = new TreeMap<>();
            for (final Map.Entry<Integer, Deque<Bytes>> e : queues.entrySet()) {
                out.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    void restoreFrom(
            @NonNull final Map<Integer, Bytes> newSingletons,
            @NonNull final Map<Integer, Map<Bytes, Bytes>> newKvs,
            @NonNull final Map<Integer, List<Bytes>> newQueues) {
        lock.writeLock().lock();
        try {
            singletons.clear();
            kvs.clear();
            queues.clear();
            singletons.putAll(newSingletons);
            for (final Map.Entry<Integer, Map<Bytes, Bytes>> e : newKvs.entrySet()) {
                kvs.put(e.getKey(), new HashMap<>(e.getValue()));
            }
            for (final Map.Entry<Integer, List<Bytes>> e : newQueues.entrySet()) {
                queues.put(e.getKey(), new ArrayDeque<>(e.getValue()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static byte[] intToBytes(final int v) {
        return new byte[] {
            (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v,
        };
    }
}
