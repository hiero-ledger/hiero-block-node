package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.FCDataInputStream;
import java.io.IOException;

public record JAccountID(long shardNum, long realmNum, long accountNum) {

    public static JAccountID copyFrom(FCDataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        long objectType = inStream.readLong();
        long shardNum = inStream.readLong();
        long realmNum = inStream.readLong();
        long accountNum = inStream.readLong();
        return new JAccountID(shardNum, realmNum, accountNum);
    }
}
