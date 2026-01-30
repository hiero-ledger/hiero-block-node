package org.hiero.block.tools.states.model;

import java.io.BufferedInputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.states.utils.CryptoUtils;
import org.hiero.block.tools.states.utils.FCDataInputStream;
import org.hiero.block.tools.states.utils.FCDataOutputStream;
import org.hiero.block.tools.states.utils.HashingOutputStream;
import org.hiero.block.tools.states.utils.Utilities;
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

    /**
     * Load a SignedState from a URL, can be compressed (.gz) or uncompressed
     *
     * @param stateFileUrl the URL to the signed state file
     * @return the loaded SignedState
     * @throws IOException if an I/O error occurs
     */
    public static SignedState load(URL stateFileUrl) throws IOException {
        SignedState signedState = new SignedState();
        try (FCDataInputStream fin = new FCDataInputStream(
            stateFileUrl.toString().endsWith(".gz") ?
                new GZIPInputStream(new BufferedInputStream(stateFileUrl.openStream(), 1024 * 1024)) :
                new BufferedInputStream(stateFileUrl.openStream(), 1024 * 1024)
        )) {
            signedState.copyFrom(fin);
            signedState.copyFromExtra(fin);
        }
        return signedState;
    }

    /**
     * Load a SignedState from a file, can be compressed (.gz) or uncompressed
     *
     * @param stateFile the path to the signed state file
     * @return the loaded SignedState
     * @throws IOException if an I/O error occurs
     */
    public static SignedState load(Path stateFile) throws IOException {
        SignedState signedState = new SignedState();
        try (FCDataInputStream fin = new FCDataInputStream(
            stateFile.getFileName().toString().endsWith(".gz") ?
                new GZIPInputStream(new BufferedInputStream(Files.newInputStream(stateFile), 1024 * 1024)) :
                new BufferedInputStream(Files.newInputStream(stateFile), 1024 * 1024)
        )) {
            signedState.copyFrom(fin);
            signedState.copyFromExtra(fin);
        }
        return signedState;
    }

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
