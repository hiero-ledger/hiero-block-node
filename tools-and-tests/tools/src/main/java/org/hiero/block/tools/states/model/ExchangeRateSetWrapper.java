package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;
import org.hiero.block.tools.states.utils.FCDataOutputStream;
import java.io.IOException;

public record ExchangeRateSetWrapper(
        int currentHbarEquiv,
        int currentCentEquiv,
        long currentExpirationTime,
        int nextHbarEquiv,
        int nextCentEquiv,
        long nextExpirationTime) {
    private static final long VERSION = 1L;
    private static final long OBJECT_ID = 10000121L;


    public static ExchangeRateSetWrapper copyFrom(FCDataInputStream fcDataInputStream) throws IOException {
        //version number
        long version = fcDataInputStream.readLong();
        if (version != VERSION) {
            throw new IOException(
                    "Read Invalid version while calling ExchangeRateSetWrapper.copyFrom() - expected: "
                            + VERSION + ", got: " + version);
        }
        long objectId = fcDataInputStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException(
                    "Read Invalid ObjectID while calling ExchangeRateSetWrapper.copyFrom()");
        }
        int currentHbarEquiv = fcDataInputStream.readInt();
        int currentCentEquiv = fcDataInputStream.readInt();
        long currentExpirationTime = fcDataInputStream.readLong();
        int nextHbarEquiv = fcDataInputStream.readInt();
        int nextCentEquiv = fcDataInputStream.readInt();
        long nextExpirationTime = fcDataInputStream.readLong();
        return new ExchangeRateSetWrapper(currentHbarEquiv, currentCentEquiv, currentExpirationTime,
                nextHbarEquiv, nextCentEquiv, nextExpirationTime);
    }

    public void copyTo(FCDataOutputStream fcDataOutputStream) throws IOException {
        fcDataOutputStream.writeLong(VERSION);
        fcDataOutputStream.writeLong(OBJECT_ID);
        fcDataOutputStream.writeInt(currentHbarEquiv);
        fcDataOutputStream.writeInt(currentCentEquiv);
        fcDataOutputStream.writeLong(currentExpirationTime);
        fcDataOutputStream.writeInt(nextHbarEquiv);
        fcDataOutputStream.writeInt(nextCentEquiv);
        fcDataOutputStream.writeLong(nextExpirationTime);
    }
}
