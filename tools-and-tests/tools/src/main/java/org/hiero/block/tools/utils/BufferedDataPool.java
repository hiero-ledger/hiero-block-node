package org.hiero.block.tools.utils;

import com.hedera.pbj.runtime.io.buffer.BufferedData;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A thread safe pool for on heap buffered data objects.
 *
 * Implementation notes:
 * - Bounded lock-free ring buffer using AtomicReferenceArray.
 * - Optimized for a single consumer (caller of getBuffer) and a single producer (caller of returnBuffer),
 *   which avoids heavy atomic contention and keeps operations fast.
 * - If a buffer of sufficient capacity isn't available the pool returns a newly allocated BufferedData.
 */
public class BufferedDataPool {
    public static final int POOL_SIZE = 100;

    // Capacity and optional mask for power-of-two sizes
    private static final int capacity = POOL_SIZE;
    private static final boolean isPowerOfTwo = (capacity & (capacity - 1)) == 0;
    private static final int mask = capacity - 1;

    private static final AtomicReferenceArray<BufferedData> items = new AtomicReferenceArray<>(capacity);
    // head points to next index to take from (consumer), tail points to next index to put to (producer)
    private static final AtomicInteger head = new AtomicInteger(0);
    private static final AtomicInteger tail = new AtomicInteger(0);

    private BufferedDataPool() {
        // utility
    }

    /**
     * Obtain a BufferedData with at least the requested capacity. If a pooled buffer is available it will be
     * returned, otherwise a new instance will be created.
     *
     * This method is safe to call from one consumer thread while another thread returns buffers via
     * {@link #returnBuffer(BufferedData)}. It's optimized for a single consumer thread.
     *
     * @param size minimum required capacity in bytes
     * @return a BufferedData instance with capacity >= size
     */
    public static BufferedData getBuffer(int size) {
        final int cap = capacity;
        final int h = head.get();
        // probe up to the entire capacity, starting from current head to avoid re-checking the same index
        for (int i = 0; i < cap; i++) {
            final int probe = h + i;
            final int idx = isPowerOfTwo ? (probe & mask) : (probe % cap);
            final BufferedData b = items.get(idx);
            if (b == null) {
                // empty slot, try next
                continue;
            }
            // try to take this buffer by CASing the slot to null
            if (items.compareAndSet(idx, b, null)) {
                // successfully removed from pool, attempt to advance head to probe+1 (best-effort)
                head.compareAndSet(probe, probe + 1);
                // ensure capacity is sufficient
                if (b.capacity() >= size) {
                    b.reset();
                    return b;
                } else {
                    // buffer too small; drop it and continue scanning
                    continue;
                }
            }
            // CAS failed; another thread interfered, try next probe
        }

        // Not found in pool; create new
        return BufferedData.wrap(new byte[size]);
    }

    /**
     * Return a BufferedData to the pool for reuse. If the pool is full the buffer is dropped and will be garbage collected.
     * The buffer is not resized; callers should only return buffers that they obtained from this pool or that are safe to reuse.
     *
     * This method is safe to call from one producer thread while another thread consumes via {@link #getBuffer(int)}.
     */
    public static void returnBuffer(BufferedData buffer) {
        if (buffer == null) return;
        final int cap = capacity;
        final int t = tail.get();
        // probe for an empty slot starting from tail
        for (int i = 0; i < cap; i++) {
            final int probe = t + i;
            final int idx = isPowerOfTwo ? (probe & mask) : (probe % cap);
            if (items.get(idx) != null) {
                // occupied, try next
                continue;
            }
            // try to place buffer here
            if (items.compareAndSet(idx, null, buffer)) {
                // successfully placed, attempt to advance tail to probe+1 (best-effort)
                tail.compareAndSet(probe, probe + 1);
                return;
            }
            // failed to CAS, try next
        }
        // pool full, drop buffer
    }
}
