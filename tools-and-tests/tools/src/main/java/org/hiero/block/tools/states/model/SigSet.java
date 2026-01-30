// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.hiero.block.tools.states.utils.FCDataInputStream;
import org.hiero.block.tools.states.utils.Utilities;

public final class SigSet {
    private long classVersion;
    private int count;
    private long stakeCollected;
    private int numMembers;
    private boolean complete = false;
    private AtomicReferenceArray<SigInfo> sigInfos;
    private final AddressBook addressBook;

    /** create the signature set, taking the address book at this moment as the population */
    SigSet(AddressBook addressBook) {
        classVersion = 1;
        this.addressBook = addressBook;
        this.numMembers = addressBook.getSize();
        sigInfos = new AtomicReferenceArray<>(numMembers);
        count = 0;
        stakeCollected = 0;
    }

    public void copyFrom(FCDataInputStream inStream) throws IOException {
        classVersion = inStream.readLong();
        numMembers = inStream.readInt();
        SigInfo[] sigInfoArr = new SigInfo[numMembers];
        try {
            sigInfoArr =
                    Utilities.readFastCopyableArray(inStream, SigInfo::copyFrom).toArray(new SigInfo[0]);
        } catch (Exception e) {
            // TODO fix this to log the error, then rethrow.
            e.printStackTrace();
        }

        count = 0;
        stakeCollected = 0;
        sigInfos = new AtomicReferenceArray<>(numMembers);
        if (sigInfos != null) {
            for (int id = 0; id < sigInfoArr.length; id++) {
                if (sigInfoArr[id] != null) {
                    sigInfos.set(id, sigInfoArr[id]);
                    count++;
                    stakeCollected += addressBook.getStake(id);
                }
            }
        }
        calculateComplete();
    }

    private void calculateComplete() {
        complete = Utilities.isSupermajority(stakeCollected, addressBook.getTotalStake());
    }

    public long classVersion() {
        return classVersion;
    }

    public int count() {
        return count;
    }

    public long stakeCollected() {
        return stakeCollected;
    }

    public int numMembers() {
        return numMembers;
    }

    public boolean complete() {
        return complete;
    }

    public AtomicReferenceArray<SigInfo> sigInfos() {
        return sigInfos;
    }

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
