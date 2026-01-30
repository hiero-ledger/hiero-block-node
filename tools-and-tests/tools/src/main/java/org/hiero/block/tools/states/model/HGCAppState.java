// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import org.hiero.block.tools.states.utils.Utils;

public class HGCAppState {
    /** since version4, ExchangeRateSetWrapper is saved in state */
    private static final long VERSION_WITH_EXCHANGE_RATE = 4;

    private static final long LEGACY_VERSION = 3;
    private static final long CURRENT_VERSION = 4;

    private SequenceNumber sequenceNum;
    private final AddressBook addressBook = new AddressBook();
    private final FCMap<MapKey, MapValue> accountMap = new FCMap<>(MapKey::copyFrom, MapValue::copyFrom);
    private final FCMap<StorageKey, StorageValue> storageMap =
            new FCMap<>(StorageKey::copyFrom, StorageValue::copyFrom);
    private ExchangeRateSetWrapper exchangeRateSetWrapper;
    private Instant lastHandleTxConsensusTime;

    public FCMap<MapKey, MapValue> accountMap() {
        return accountMap;
    }

    public FCMap<StorageKey, StorageValue> storageMap() {
        return storageMap;
    }

    public HGCAppState copyFrom(DataInputStream DataInputStream) throws IOException {
        long savedVersion = DataInputStream.readLong();
        sequenceNum = SequenceNumber.copyFrom(DataInputStream);
        addressBook.copyFrom(DataInputStream);
        accountMap.copyFrom(DataInputStream);
        storageMap.copyFrom(DataInputStream);

        if (savedVersion >= VERSION_WITH_EXCHANGE_RATE) {
            if (DataInputStream.readBoolean()) {
                exchangeRateSetWrapper = ExchangeRateSetWrapper.copyFrom(DataInputStream);
                System.out.println(
                        "Loaded exchange rates from state via HGCAppState::copyFrom() - " + exchangeRateSetWrapper);
            } else {
                System.out.println("No exchange rates were loaded from state via HGCAppState::copyFrom()");
            }

            if (DataInputStream.readBoolean()) {
                lastHandleTxConsensusTime = Utils.readInstant(DataInputStream);
            }
        }
        return this;
    }

    public void copyFromExtra(DataInputStream DataInputStream) throws IOException {
        long version = DataInputStream.readLong();
        if (version < LEGACY_VERSION || version > CURRENT_VERSION) {
            throw new IOException("Unsupported HGCAppState version: " + version);
        }
        accountMap.copyFromExtra(DataInputStream);
        storageMap.copyFromExtra(DataInputStream);
    }

    public synchronized void copyTo(DataOutputStream DataOutputStream) throws IOException {
        DataOutputStream.writeLong(CURRENT_VERSION);
        // fcFileSystem.copyTo(DataOutputStream);
        sequenceNum.copyTo(DataOutputStream);
        addressBook.copyTo(DataOutputStream);
        accountMap.copyTo(DataOutputStream);
        storageMap.copyTo(DataOutputStream);

        if (exchangeRateSetWrapper == null) {
            DataOutputStream.writeBoolean(false);
        } else {
            DataOutputStream.writeBoolean(true);
            exchangeRateSetWrapper.copyTo(DataOutputStream);
        }

        if (lastHandleTxConsensusTime == null) {
            DataOutputStream.writeBoolean(false);
        } else {
            DataOutputStream.writeBoolean(true);
            Utils.writeInstant(DataOutputStream, lastHandleTxConsensusTime);
        }
    }
}
