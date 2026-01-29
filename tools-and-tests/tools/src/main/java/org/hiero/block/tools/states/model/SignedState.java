package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.CryptoUtils;
import org.hiero.block.tools.states.FCDataInputStream;
import org.hiero.block.tools.states.FCDataOutputStream;
import org.hiero.block.tools.states.HashingOutputStream;
import org.hiero.block.tools.states.Utilities;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("CallToPrintStackTrace")
public final class SignedState {
    /** This version number should be used to handle compatibility issues that may arise from any future changes */
    private static final long CLASS_VERSION = 2;

    private long instanceVersion;
    private byte[] readHash;
    private final HGCAppState state = new HGCAppState();
    private long round;
    private long numEventsCons = 0;
    private final AddressBook addressBook = new AddressBook();
    private byte[] hashEventsCons;
    private Instant consensusTimestamp;
    private boolean shouldSaveToDisk;
    private Event[] events;
    private List<Pair<Long, Long>> minGenInfo;
    private SigSet sigSet;

    public SignedState() {}

    public byte[] readHash() {
        return readHash;
    }

    public long instanceVersion() {
        return instanceVersion;
    }

    public long round() {
        return round;
    }

    public long numEventsCons() {
        return numEventsCons;
    }

    public AddressBook addressBook() {
        return addressBook;
    }

    public boolean shouldSaveToDisk() {
        return shouldSaveToDisk;
    }

    public HGCAppState state() {
        return state;
    }

    public Instant consensusTimestamp() {
        return consensusTimestamp;
    }

    public void copyFrom(FCDataInputStream inStream) throws IOException {
        instanceVersion = inStream.readLong();// classVersion
        if (instanceVersion != CLASS_VERSION) {
            throw new IOException("SignedState version mismatch: expected " + CLASS_VERSION + ", got " + instanceVersion);
        }
        readHash = Utilities.readByteArray(inStream);
        state.copyFrom(inStream);
        round = inStream.readLong();
        System.out.println("    round = " + round);
        // check round, should be 33312259 which is OA round
//        if (round != 33312259) {
//            throw new IOException("Invalid round number: " + round+" expected 33312259");
//        }
        numEventsCons = inStream.readLong();
        addressBook.copyFrom(inStream);
        hashEventsCons = Utilities.readByteArray(inStream);
        consensusTimestamp = Utilities.readInstant(inStream);
        System.out.println("    consensusTimestamp = " + consensusTimestamp);
        shouldSaveToDisk = inStream.readBoolean();

        int numEvents = inStream.readInt();
        System.out.println("    numEvents = " + numEvents);
        events = new Event[numEvents];
        HashMap<CreatorSeqPair, Event> eventsByCreatorSeq = new HashMap<>();
        for (int i = 0; i < events.length; i++) {
            events[i] = Event.readFrom(inStream, eventsByCreatorSeq);
            eventsByCreatorSeq.put(new CreatorSeqPair(events[i].creatorId(),
                    events[i].creatorSeq()), events[i]);
        }

        // from version 2 we store the minGen info
        if (instanceVersion >= 2) {
            minGenInfo = Utilities.readList(inStream, LinkedList::new, (stream) -> {
                long key = stream.readLong();
                long value = stream.readLong();
                return new Pair<>(key, value);
            });
        } else {
            // if we don't have min gen info, we need to produce it from the events
            minGenInfo = produceMinGenFromEvents();
        }

        sigSet = new SigSet(addressBook);
        sigSet.copyFrom(inStream);
    }

    public void copyFromExtra(FCDataInputStream inStream) throws IOException {
        state.copyFromExtra(inStream);
    }


    private static final  long roundsStale = 25;
    private List<Pair<Long, Long>> produceMinGenFromEvents() {
        long minGen = events[0].generation();
        List<Pair<Long, Long>> list = new LinkedList<>();
        for (long i = round - roundsStale; i <= round; i++) {
            list.add(new Pair<>(i, minGen));
        }
        return list;
    }

    public byte[] generateSignedStateHash() {
        MessageDigest digest = CryptoUtils.getMessageDigest();
        byte[] swirldStateHash = generateSwirldStateHash(digest);
        assert swirldStateHash != null;
        System.out.println("swirldStateHash = " + HexFormat.of().formatHex(swirldStateHash));
        digest.reset();

        Hash.update(digest, this.round);
        Hash.update(digest, consensusTimestamp);
        Hash.update(digest, this.numEventsCons);
        digest.update(this.hashEventsCons);
        for (int i = 0; i < events.length; i++) {
            digest.update(events[i].getHash());
        }
        digest.update(swirldStateHash);
        digest.update(addressBook.getHash());
        return digest.digest();
    }

    /**
     * Generates a hash of the SwirldState
     *
     * @param digest
     * 		the object to do the hashing
     * @return the hash generated, or null if an error occurred
     */
    public byte[] generateSwirldStateHash(MessageDigest digest) {
        // the streams are closed automatically
        try (HashingOutputStream hashOut = new HashingOutputStream(digest);
                BufferedOutputStream bufOut = new BufferedOutputStream(hashOut);
                FCDataOutputStream fcOut = new FCDataOutputStream(bufOut)) {
            state.copyTo(fcOut);
            fcOut.flush();
            return digest.digest();
        } catch (IOException e) {
            e.printStackTrace();
        }
        digest.reset();
        return null;
    }

    @Override
    public String toString() {
        return "SignedState{" +
                "instanceVersion=" + instanceVersion +
                ",\n     readHash=" + HexFormat.of().formatHex(readHash) +
                ",\n     state=" + state +
                ",\n     round=" + round +
                ",\n     numEventsCons=" + numEventsCons +
                ",\n     addressBook=" + addressBook +
                ",\n     hashEventsCons=" + HexFormat.of().formatHex(hashEventsCons) +
                ",\n     consensusTimestamp=" + consensusTimestamp +
                ",\n     shouldSaveToDisk=" + shouldSaveToDisk +
                ",\n     events Count=" + events.length +
                ",\n     minGenInfo=" + minGenInfo +
                ",\n     sigSet=" + sigSet +
                "\n}";
    }
}
