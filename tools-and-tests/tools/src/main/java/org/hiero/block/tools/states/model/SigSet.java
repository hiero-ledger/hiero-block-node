// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.hiero.block.tools.states.utils.Utils;

/** A set of member signatures collected for a signed state, tracking stake-weighted supermajority. */
public final class SigSet {
    /** The class version for serialization. */
    private long classVersion;
    /** The count of signatures collected. */
    private int count;
    /** The total stake represented by collected signatures. */
    private long stakeCollected;
    /** The total number of members in the address book. */
    private int numMembers;
    /** Whether a supermajority of signatures has been collected. */
    private boolean complete = false;
    /** The array of signature info objects indexed by member ID. */
    private AtomicReferenceArray<SigInfo> sigInfos;
    /** The address book defining the member population and stake distribution. */
    private final AddressBook addressBook;

    /**
     * Creates a new signature set for the given address book.
     *
     * @param addressBook the address book defining the member population
     */
    SigSet(AddressBook addressBook) {
        classVersion = 1;
        this.addressBook = addressBook;
        this.numMembers = addressBook.getSize();
        sigInfos = new AtomicReferenceArray<>(numMembers);
        count = 0;
        stakeCollected = 0;
    }

    /**
     * Deserializes this signature set from the given stream.
     *
     * @param inStream the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public void copyFrom(DataInputStream inStream) throws IOException {
        classVersion = inStream.readLong();
        numMembers = inStream.readInt();
        SigInfo[] sigInfoArr;
        try {
            sigInfoArr =
                    Utils.readFastCopyableArray(inStream, SigInfo::copyFrom).toArray(new SigInfo[0]);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        count = 0;
        stakeCollected = 0;
        sigInfos = new AtomicReferenceArray<>(numMembers);
        for (int id = 0; id < sigInfoArr.length; id++) {
            if (sigInfoArr[id] != null) {
                sigInfos.set(id, sigInfoArr[id]);
                count++;
                stakeCollected += addressBook.getStake(id);
            }
        }
        calculateComplete();
    }

    private void calculateComplete() {
        complete = Utils.isSupermajority(stakeCollected, addressBook.getTotalStake());
    }

    /**
     * Returns the count of signatures collected.
     *
     * @return the signature count
     */
    public int count() {
        return count;
    }

    /**
     * Returns whether a supermajority of signatures has been collected.
     *
     * @return {@code true} if the signature set is complete
     */
    public boolean complete() {
        return complete;
    }

    /**
     * Returns the total number of members in the address book.
     *
     * @return the member count
     */
    public int numMembers() {
        return numMembers;
    }

    /**
     * Returns the signature info for the member at the given index.
     *
     * @param index the member index
     * @return the signature info, or {@code null} if the member has not signed
     */
    public SigInfo sigInfo(int index) {
        return sigInfos.get(index);
    }

    /**
     * Returns the total stake represented by collected signatures.
     *
     * @return the stake collected
     */
    public long stakeCollected() {
        return stakeCollected;
    }

    /**
     * Returns the address book defining the member population.
     *
     * @return the address book
     */
    public AddressBook addressBook() {
        return addressBook;
    }

    @Override
    public String toString() {
        return "SigSet{\n" + "                classVersion="
                + classVersion + ",\n        count="
                + count + ",\n        stakeCollected="
                + stakeCollected + ",\n        numMembers="
                + numMembers + ",\n        complete="
                + complete + ",\n        sigInfos="
                + sigInfos.toString().replaceAll("SigInfo", "\n            SigInfo") + ",\n        addressBook="
                + addressBook + '}';
    }
}
