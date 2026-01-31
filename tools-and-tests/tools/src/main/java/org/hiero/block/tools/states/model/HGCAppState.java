// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import org.hiero.block.tools.states.utils.Utils;

/** The top-level Hedera application state containing account map, storage map, and exchange rates. */
public class HGCAppState {
    /** since version4, ExchangeRateSetWrapper is saved in state */
    private static final long VERSION_WITH_EXCHANGE_RATE = 4;

    /** legacy state version (3) prior to exchange rate support */
    private static final long LEGACY_VERSION = 3;
    /** current state version (4) with exchange rate support */
    private static final long CURRENT_VERSION = 4;

    /** the global sequence number for this state */
    private SequenceNumber sequenceNum;
    /** the network address book */
    private final AddressBook addressBook = new AddressBook();
    /** the account ledger mapping account keys to account values */
    private final FCMap<MapKey, MapValue> accountMap = new FCMap<>(MapKey::copyFrom, MapValue::copyFrom);
    /** the contract storage mapping storage keys to storage values */
    private final FCMap<StorageKey, StorageValue> storageMap =
            new FCMap<>(StorageKey::copyFrom, StorageValue::copyFrom);
    /** the exchange rate set wrapper, if present */
    private ExchangeRateSetWrapper exchangeRateSetWrapper;
    /** the consensus timestamp of the last handled transaction */
    private Instant lastHandleTxConsensusTime;

    /**
     * Gets the account FCMap.
     *
     * @return the account FCMap
     */
    public FCMap<MapKey, MapValue> accountMap() {
        return accountMap;
    }

    /**
     * Gets the storage FCMap.
     *
     * @return the storage FCMap
     */
    public FCMap<StorageKey, StorageValue> storageMap() {
        return storageMap;
    }

    /**
     * Deserializes the HGCAppState header from the given stream.
     *
     * @param DataInputStream the stream to read from
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
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

    /**
     * Deserializes the HGCAppState contents from the given stream.
     *
     * @param DataInputStream the stream to read from
     * @throws IOException if an I/O error occurs or the version is unsupported
     */
    public void copyFromExtra(DataInputStream DataInputStream) throws IOException {
        long version = DataInputStream.readLong();
        if (version < LEGACY_VERSION || version > CURRENT_VERSION) {
            throw new IOException("Unsupported HGCAppState version: " + version);
        }
        accountMap.copyFromExtra(DataInputStream);
        storageMap.copyFromExtra(DataInputStream);
    }

    /**
     * Serializes this HGCAppState to the given stream.
     *
     * @param DataOutputStream the stream to write to
     * @throws IOException if an I/O error occurs
     */
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
