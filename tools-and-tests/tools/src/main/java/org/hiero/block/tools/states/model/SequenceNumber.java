// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import org.hiero.block.tools.states.utils.FCDataInputStream;
import org.hiero.block.tools.states.utils.FCDataOutputStream;

public record SequenceNumber(long sequenceNum) {

    public static SequenceNumber copyFrom(FCDataInputStream inStream) throws IOException {
        return new SequenceNumber(inStream.readLong());
    }

    public void copyFromExtra(FCDataInputStream arg0) throws IOException {
        // empty implementation
    }

    public void copyTo(FCDataOutputStream fcDataOutputStream) throws IOException {
        fcDataOutputStream.writeLong(sequenceNum);
    }
}
