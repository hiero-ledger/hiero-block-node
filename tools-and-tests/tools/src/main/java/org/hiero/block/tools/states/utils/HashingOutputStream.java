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
import org.jspecify.annotations.NonNull;

/**
 * An OutputStream which creates a hash of all the bytes that go through it
 */
public class HashingOutputStream extends OutputStream {
    MessageDigest md;

    /**
     * A constructor used to create an OutputStream that only computes a hash.
     *
     * @param md
     * 		the MessageDigest object that does the hashing
     */
    public HashingOutputStream(MessageDigest md) {
        super();
        this.md = md;
    }

    @Override
    public void write(int arg0) throws IOException {
        md.update((byte) arg0);
    }

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
