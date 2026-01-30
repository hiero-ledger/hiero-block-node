// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public record SequenceNumber(long sequenceNum) {

    public static SequenceNumber copyFrom(DataInputStream inStream) throws IOException {
        return new SequenceNumber(inStream.readLong());
    }

    public void copyTo(DataOutputStream DataOutputStream) throws IOException {
        DataOutputStream.writeLong(sequenceNum);
    }
}
