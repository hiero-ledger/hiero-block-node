// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import java.util.Iterator;
import java.util.NoSuchElementException;

/// Single-use iterator that delivers one byte array and then reports exhausted.
final class PayloadIterator implements Iterator<byte[]> {
    private final byte[] payload;
    private boolean delivered = false;

    PayloadIterator(final byte[] payload) {
        this.payload = payload;
    }

    @Override
    public boolean hasNext() {
        return !delivered;
    }

    @Override
    public byte[] next() {
        if (delivered) {
            throw new NoSuchElementException();
        }
        delivered = true;
        return payload;
    }
}
