// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;
/**
 * Marker interface that controls whether a handler participates as a gating sequence on the
 * underlying LMAX Disruptor ring buffer.
 *
 * <p>When a handler is <em>gating</em> ({@link #isGating()} returns {@code true}), the ring
 * buffer will not overwrite any slot until the handler has consumed it.  This provides
 * backpressure: a slow handler stalls the publisher once the buffer is full.  Use this for
 * handlers that <em>must</em> process every event (e.g. block persistence, verification).
 *
 * <p>When a handler is <em>non-gating</em> ({@link #isGating()} returns {@code false}), the
 * handler's sequence is <strong>not</strong> added to the ring buffer's gating sequences.  The
 * publisher is never stalled by a slow or absent handler.  Additionally, the processor is
 * initialized at the ring buffer's current cursor so it only receives <em>future</em> events;
 * past events already in the buffer are not replayed.  Use this for best-effort consumers such
 * as live-streaming subscribers that must not impede block ingestion.
 *
 * <p>Both {@link BlockItemHandler} and {@link NoBackPressureBlockItemHandler} extend this
 * interface; the default implementation here returns {@code true} (gating), and
 * {@link NoBackPressureBlockItemHandler} overrides it to return {@code false}.
 */
public interface GatingHandler {

    /**
     * Returns {@code true} if this handler should act as a gating sequence on the ring buffer,
     * applying backpressure to publishers when the handler falls behind; {@code false} if the
     * handler should receive only future events without ever stalling the publisher.
     *
     * @return {@code true} for gating (backpressure) semantics; {@code false} for no-backpressure
     *         (best-effort, future-only) semantics
     */
    default boolean isGating() {
        return true;
    }
}
