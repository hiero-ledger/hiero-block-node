// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.ack;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple block status object that:
 *  - Uses volatile booleans for 'persisted' and 'verified' (set once, from false to true).
 *  - Uses an AtomicBoolean 'ackSent' for lock-free compare-and-set if a block has been ACKed.
 */
public class AckBlockStatus



{

    private volatile boolean persisted = false;
    private volatile boolean verified = false;

    /** Flag that tracks whether this block has been ACKed. */
    private final AtomicBoolean ackSent = new AtomicBoolean(false);

    /**
     * Marks this block as persisted.
     * This is a "set once" transition from false -> true (idempotent if called again).
     */
    public void setPersisted() {
        persisted = true;
    }

    /**
     * Marks this block as verified.
     * This is a "set once" transition from false -> true (idempotent if called again).
     */
    public void setVerified() {
        verified = true;
    }

    /**
     * Atomically marks this block as ACKed if not already done.
     *
     * @return true if this call successfully set 'ackSent' from false -> true,
     *         false if 'ackSent' was already true.
     */
    public boolean markAckSentIfNotAlready() {
        return ackSent.compareAndSet(false, true);
    }

    /**
     * @return true if persisted = true
     */
    public boolean isPersisted() {
        return persisted;
    }

    /**
     * @return true if verified = true
     */
    public boolean isVerified() {
        return verified;
    }

    /**
     * @return true if 'ackSent' has already been set
     */
    public boolean isAckSent() {
        return ackSent.get();
    }
}
