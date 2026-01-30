// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import java.io.DataInputStream;

@SuppressWarnings("unused")
public record JAccountID(long shardNum, long realmNum, long accountNum) {

    public static JAccountID copyFrom(DataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        long objectType = inStream.readLong();
        long shardNum = inStream.readLong();
        long realmNum = inStream.readLong();
        long accountNum = inStream.readLong();
        return new JAccountID(shardNum, realmNum, accountNum);
    }
}
