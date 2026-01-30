package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;
import org.hiero.block.tools.states.utils.FCDataOutputStream;
import org.hiero.block.tools.states.utils.Utilities;
import java.io.IOException;
import java.time.Instant;

public class HGCAppState {
    /** since version4, ExchangeRateSetWrapper is saved in state */
    private static final long VERSION_WITH_EXCHANGE_RATE = 4;
    private static final long LEGACY_VERSION = 3;
    private static final long CURRENT_VERSION = 4;

    private SequenceNumber sequenceNum;
    private final AddressBook addressBook = new AddressBook();
    private final FCMap<MapKey, MapValue> accountMap = new FCMap<>(MapKey::copyFrom, MapValue::copyFrom);
    private final FCMap<StorageKey, StorageValue> storageMap = new FCMap<>(StorageKey::copyFrom, StorageValue::copyFrom);
    private ExchangeRateSetWrapper exchangeRateSetWrapper;
    private Instant lastHandleTxConsensusTime;

    public FCMap<MapKey, MapValue> accountMap() {
        return accountMap;
    }

    public FCMap<StorageKey, StorageValue> storageMap() {
        return storageMap;
    }

    public HGCAppState copyFrom(FCDataInputStream fcDataInputStream) throws IOException {
        long savedVersion = fcDataInputStream.readLong();
        sequenceNum = SequenceNumber.copyFrom(fcDataInputStream);
        addressBook.copyFrom(fcDataInputStream);
        accountMap.copyFrom(fcDataInputStream);
        storageMap.copyFrom(fcDataInputStream);

        if (savedVersion >= VERSION_WITH_EXCHANGE_RATE) {
            if (fcDataInputStream.readBoolean()) {
                exchangeRateSetWrapper = ExchangeRateSetWrapper.copyFrom(fcDataInputStream);
                System.out.println("Loaded exchange rates from state via HGCAppState::copyFrom() - "+exchangeRateSetWrapper);
            } else {
                System.out.println("No exchange rates were loaded from state via HGCAppState::copyFrom()");
            }

            if (fcDataInputStream.readBoolean()) {
                lastHandleTxConsensusTime = Utilities.readInstant(fcDataInputStream);
            }
        }
        return this;
    }

    public void copyFromExtra(FCDataInputStream fcDataInputStream) throws IOException {
        long version = fcDataInputStream.readLong();
        if (version < LEGACY_VERSION || version > CURRENT_VERSION) {
            throw new IOException("Unsupported HGCAppState version: " + version);
        }
        sequenceNum.copyFromExtra(fcDataInputStream);
        addressBook.copyFromExtra(fcDataInputStream);
        accountMap.copyFromExtra(fcDataInputStream);
        storageMap.copyFromExtra(fcDataInputStream);
    }

    public synchronized void copyTo(FCDataOutputStream fcDataOutputStream) throws IOException {
        fcDataOutputStream.writeLong(CURRENT_VERSION);
        // fcFileSystem.copyTo(fcDataOutputStream);
        sequenceNum.copyTo(fcDataOutputStream);
        addressBook.copyTo(fcDataOutputStream);
        accountMap.copyTo(fcDataOutputStream);
        storageMap.copyTo(fcDataOutputStream);

        if (exchangeRateSetWrapper == null) {
            fcDataOutputStream.writeBoolean(false);
        } else {
            fcDataOutputStream.writeBoolean(true);
            exchangeRateSetWrapper.copyTo(fcDataOutputStream);
        }

        if (lastHandleTxConsensusTime == null) {
            fcDataOutputStream.writeBoolean(false);
        } else {
            fcDataOutputStream.writeBoolean(true);
            Utilities.writeInstant(fcDataOutputStream, lastHandleTxConsensusTime);
        }
    }


}
