package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.FCDataInputStream;
import org.hiero.block.tools.states.FCDataOutputStream;
import java.io.IOException;

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
