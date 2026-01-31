// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A serializable sequence number used for ordering within the Hedera state.
 *
 * @param sequenceNum the sequence number value
 */
public record SequenceNumber(long sequenceNum) {

    /**
     * Deserializes a SequenceNumber from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized SequenceNumber
     * @throws IOException if an I/O error occurs
     */
    public static SequenceNumber copyFrom(DataInputStream inStream) throws IOException {
        return new SequenceNumber(inStream.readLong());
    }

    /**
     * Serializes this SequenceNumber to the given stream.
     *
     * @param DataOutputStream the stream to write to
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(DataOutputStream DataOutputStream) throws IOException {
        DataOutputStream.writeLong(sequenceNum);
    }
}
