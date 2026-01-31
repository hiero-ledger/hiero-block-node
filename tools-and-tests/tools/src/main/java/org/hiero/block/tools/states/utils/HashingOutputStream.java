// SPDX-License-Identifier: Apache-2.0
/*
 * (c) 2016-2018 Swirlds, Inc.
 *
 * This software is the confidential and proprietary information of
 * Swirlds, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Swirlds.
 *
 * SWIRLDS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SWIRLDS SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package org.hiero.block.tools.states.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;

/**
 * An {@link OutputStream} that computes a cryptographic hash of all bytes written through it.
 *
 * <p>This stream does not write data to any underlying destination; it only feeds the bytes into a
 * {@link MessageDigest} for hash computation. After all data has been written, the caller can
 * retrieve the final hash by calling {@link MessageDigest#digest()} on the digest instance that
 * was provided at construction time.
 */
public class HashingOutputStream extends OutputStream {
    MessageDigest md;

    /**
     * Creates a new {@code HashingOutputStream} that feeds all written bytes into the given
     * {@link MessageDigest}.
     *
     * @param md the {@link MessageDigest} instance used to compute the hash of all written bytes
     */
    public HashingOutputStream(MessageDigest md) {
        super();
        this.md = md;
    }

    /**
     * Writes a single byte to the message digest. The byte is the low-order 8 bits of the
     * given {@code int} value.
     *
     * @param arg0 the byte value to hash (only the low-order 8 bits are used)
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(int arg0) throws IOException {
        md.update((byte) arg0);
    }

    /**
     * Writes a portion of a byte array to the message digest.
     *
     * @param b the byte array containing the data to hash
     * @param off the start offset within the array
     * @param len the number of bytes to hash
     * @throws NullPointerException if {@code b} is {@code null}
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is negative, or if
     *         {@code off + len} exceeds the length of the array
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        md.update(b, off, len);
    }
}
